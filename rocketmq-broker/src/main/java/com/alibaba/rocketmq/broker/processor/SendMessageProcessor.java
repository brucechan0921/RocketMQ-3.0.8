/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.broker.processor;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.digestlog.SendbackmsgLiveMoniter;
import com.alibaba.rocketmq.broker.digestlog.SendmsgLiveMoniter;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.TopicFilterType;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQRequestCode;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQResponseCode;
import com.alibaba.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SendMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SendMessageResponseHeader;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode;
import com.alibaba.rocketmq.store.MessageExtBrokerInner;
import com.alibaba.rocketmq.store.PutMessageResult;


/**
 * 处理客户端发送消息的请求
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class SendMessageProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final static int DLQ_NUMS_PER_GROUP = 1;
    private final BrokerController brokerController;
    private final Random random = new Random(System.currentTimeMillis());
    private final SocketAddress storeHost;


    public SendMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.storeHost =
                new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(), brokerController
                    .getNettyServerConfig().getListenPort());
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        MQRequestCode code = MQRequestCode.valueOf(request.getCode());
        switch (code) {
        case SEND_MESSAGE:
        	/*
        	 * chen.si 消息发送事件，包括 普通消息、事务消息prepare/commit/rollback、定时消息
        	 */
            return this.sendMessage(ctx, request);
        case CONSUMER_SEND_MSG_BACK:
        	/*
        	 * chen.si consumer消息处理失败，直接发回给 消息所在的 broker
        	 *
        	 * 参考MQConsumer.sendMessageBack注释：
        	 * Consumer消费失败的消息可以选择重新发回到服务器端，并延时消费
        	 * 会首先尝试将消息发回到消息之前存储的主机，此时只传送消息Offset，消息体不传送，不会占用网络带宽
        	 * 如果发送失败，会自动重试发往其他主机，此时消息体也会传送
        	 * 重传回去的消息只会被当前Consumer Group消费。
        	 */
            return this.consumerSendMsgBack(ctx, request);
        default:
            break;
        }
        return null;
    }


    private RemotingCommand consumerSendMsgBack(final ChannelHandlerContext ctx, final RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final ConsumerSendMsgBackRequestHeader requestHeader =
                (ConsumerSendMsgBackRequestHeader) request
                    .decodeCommandCustomHeader(ConsumerSendMsgBackRequestHeader.class);

        /*
         * chen.si 为每个consumer group建立 重试分区队列，其中topic为%RETRY + consumerGroupName
         * 			因为一种consumer group处理消息失败， 只能放在自己专属的重试队列里，不能影响 其他的consumer group
         */
        // 确保订阅组存在
        SubscriptionGroupConfig subscriptionGroupConfig =
                this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(
                    requestHeader.getGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(MQResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST_VALUE);
            response.setRemark("subscription group not exist, " + requestHeader.getGroup() + " "
                    + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
            return response;
        }

        /*
         * chen.si 此消息不需要重试的意思
         */
        // 如果重试队列数目为0，则直接丢弃消息
        if (subscriptionGroupConfig.getRetryQueueNums() <= 0) {
            response.setCode(ResponseCode.SUCCESS_VALUE);
            response.setRemark(null);
            return response;
        }

        /*
         * chen.si 重试的消息，作为特殊的topic：%RETRY% + consumerGroup
         */
        String newTopic = MixAll.getRetryTopic(requestHeader.getGroup());
        /*
         * chen.si 随便扔到一个重试队列里
         */
        int queueIdInt =
                Math.abs(this.random.nextInt() % 99999999) % subscriptionGroupConfig.getRetryQueueNums();

        /*
         * chen.si 重试的topic还没有，创建一个
         */
        // 检查topic是否存在
        TopicConfig topicConfig =
                this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(//
                    newTopic,//
                    subscriptionGroupConfig.getRetryQueueNums(), //
                    PermName.PERM_WRITE | PermName.PERM_READ);
        if (null == topicConfig) {
            response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
            response.setRemark("topic[" + newTopic + "] not exist");
            return response;
        }

        // 检查topic权限
        if (!PermName.isWriteable(topicConfig.getPerm())) {
            response.setCode(MQResponseCode.NO_PERMISSION_VALUE);
            response.setRemark("the topic[" + newTopic + "] sending message is forbidden");
            return response;
        }

        // 查询消息，这里如果堆积消息过多，会访问磁盘
        // 另外如果频繁调用，是否会引起gc问题，需要关注 TODO
        /*
         * chen.si 发回的消息里，只有commit offset，根据commit offset寻找消息
         */
        MessageExt msgExt =
                this.brokerController.getMessageStore().lookMessageByOffset(requestHeader.getOffset());
        if (null == msgExt) {
            response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
            response.setRemark("look message by offset failed, " + requestHeader.getOffset());
            return response;
        }

        // 构造消息
        final String retryTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
        if (null == retryTopic) {
        	/*
        	 * chen.si 保留此消息的原始topic
        	 */
            msgExt.putProperty(MessageConst.PROPERTY_RETRY_TOPIC, msgExt.getTopic());
        }
        /*
         * chen.si 不用等了
         */
        msgExt.setWaitStoreMsgOK(false);

        // 客户端自动决定定时级别
        int delayLevel = requestHeader.getDelayLevel();

        // 死信消息处理
        if (msgExt.getReconsumeTimes() >= subscriptionGroupConfig.getRetryMaxTimes()//
                || delayLevel < 0) {
        	/*
        	 * chen.si 为每个consumer group，创建死信队列，名称为  %DLQ% + consumerGroupName
        	 * 		这里的死信消息无需处理了，直接扔到 死信topic和队列中 %DLQ%
        	 */
            newTopic = MixAll.getDLQTopic(requestHeader.getGroup());
            /*
             * chen.si 只有1个队列
             */
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % DLQ_NUMS_PER_GROUP;

            /*
             * chen.si 死信topic还没有，创建1个
             */
            topicConfig =
                    this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                        newTopic, //
                        DLQ_NUMS_PER_GROUP,//
                        PermName.PERM_WRITE);
            if (null == topicConfig) {
                response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
                response.setRemark("topic[" + newTopic + "] not exist");
                return response;
            }
        }
        // 继续重试
        else {
        	/*
        	 * chen.si 说明还可以重试，用定时消息 的方式 来重试
        	 */
            if (0 == delayLevel) {
                delayLevel = 3 + msgExt.getReconsumeTimes();
            }

            /*
             * chen.si 消息的延迟级别是通过 properties 的 键值 来存储的，直接设置这个
             */
            msgExt.setDelayTimeLevel(delayLevel);
        }

        /*
         * chen.si 重新存储消息，commit log中 以及 定时队列中
         */
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(newTopic);
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        msgInner.setProperties(msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(null, msgExt.getTags()));

        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes() + 1);

        PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        SendbackmsgLiveMoniter.printProcessSendmsgRequestLive(ctx.channel(), request, putMessageResult,
            delayLevel, msgExt.getReconsumeTimes());
        if (putMessageResult != null) {
            switch (putMessageResult.getPutMessageStatus()) {
            case PUT_OK:
                response.setCode(ResponseCode.SUCCESS_VALUE);
                response.setRemark(null);
                return response;
            default:
                break;
            }

            response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
            response.setRemark(putMessageResult.getPutMessageStatus().name());
            return response;
        }

        response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
        response.setRemark("putMessageResult is null");
        return response;
    }


    private String diskUtil() {
        String storePathPhysic = this.brokerController.getMessageStoreConfig().getStorePathCommitLog();
        double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);

        String storePathLogis = this.brokerController.getMessageStoreConfig().getStorePathConsumeQueue();
        double logisRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogis);

        String storePathIndex = this.brokerController.getMessageStoreConfig().getStorePathIndex();
        double indexRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathIndex);

        return String.format("CL: %5.2f CQ: %5.2f INDEX: %5.2f", physicRatio, logisRatio, indexRatio);
    }


    private RemotingCommand sendMessage(final ChannelHandlerContext ctx, final RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        final SendMessageResponseHeader responseHeader =
                (SendMessageResponseHeader) response.getCustomHeader();
        final SendMessageRequestHeader requestHeader =
                (SendMessageRequestHeader) request.decodeCommandCustomHeader(SendMessageRequestHeader.class);

        // 由于有直接返回的逻辑，所以必须要设置
        response.setOpaque(request.getOpaque());

        if (log.isDebugEnabled()) {
            log.debug("receive SendMessage request command, " + request);
        }

        /**
         * chen.si 判断当前broker是否有 写 权限
         */
        // 检查Broker权限
        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(MQResponseCode.NO_PERMISSION_VALUE);
            response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                    + "] sending message is forbidden");
            return response;
        }

        final byte[] body = request.getBody();

        /**
         * chen.si 保留字包括2个： TBW102  和  broker-cluster-name
         */
        // Topic名字是否与保留字段冲突
        if (!this.brokerController.getTopicConfigManager().isTopicCanSendMessage(requestHeader.getTopic())) {
            String errorMsg =
                    "the topic[" + requestHeader.getTopic() + "] is conflict with system reserved words.";
            log.warn(errorMsg);
            response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
            response.setRemark(errorMsg);
            return response;
        }

        // 检查topic是否存在
        TopicConfig topicConfig =
                this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            log.warn("the topic " + requestHeader.getTopic() + " not exist, producer: "
                    + ctx.channel().remoteAddress());
            
            /**
             * chen.si 可以自动创建topic
             */
            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageMethod(//
                requestHeader.getTopic(), //
                requestHeader.getDefaultTopic(), //
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()), //
                requestHeader.getDefaultTopicQueueNums());

            // 尝试看下是否是失败消息发回
            if (null == topicConfig) {
            	/**
            	 * chen.si 如果有失败需要重试的消息，正常情况下会发回给 之前消息发出的broker。 但是如果原有的broker失败，则会自动调整到其他的可用broker。
            	 * 		   因为是重试的消息，所以消息的topic为 %RETRY%xxTopic 的模式。但是是以send发方式发的，而不是sendback
            	 * 
            	 * TODO 需要看下这里的RETRY具体工作流程，先贴一个topics.json：
            	 * 
            	 * "%RETRY%benchmark_consumer":{
                        "perm":6,
                        "readQueueNums":1,
                        "topicFilterType":"SINGLE_TAG",
                        "topicName":"%RETRY%benchmark_consumer",
                        "writeQueueNums":1
                },
            	 */
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                	/**
                	 * chen.si 同是创建topic，但是这里是因为 消息消费失败而引起的sendback，只有1个read/write队列
                	 */
                    topicConfig =
                            this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                                requestHeader.getTopic(), 1, PermName.PERM_WRITE | PermName.PERM_READ);
                }
            }

            if (null == topicConfig) {
                response.setCode(MQResponseCode.TOPIC_NOT_EXIST_VALUE);
                response.setRemark("topic[" + requestHeader.getTopic() + "] not exist, apply first please!"
                        + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
                return response;
            }
        }

        // 检查topic权限
        /**
         * chen.si 比如：
         * "TBW102":{
                        "perm":7,
                        "readQueueNums":8,
                        "topicFilterType":"SINGLE_TAG",
                        "topicName":"TBW102",
                        "writeQueueNums":8
            	其中perm 对于 READ=4 WRITE=2 INHERIT=1
         */
        if (!PermName.isWriteable(topicConfig.getPerm())) {
            response.setCode(MQResponseCode.NO_PERMISSION_VALUE);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] sending message is forbidden");
            return response;
        }

        // 检查队列有效性
        /**
         * chen.si 队列，也就是消息的逻辑分区
         */
        int queueIdInt = requestHeader.getQueueId();
        if (queueIdInt >= topicConfig.getWriteQueueNums()) {
            String errorInfo =
                    "queueId[" + queueIdInt + "] is illagal, topicConfig.writeQueueNums: "
                            + topicConfig.getWriteQueueNums() + " producer: " + ctx.channel().remoteAddress();
            log.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
            response.setRemark(errorInfo);
            return response;
        }

        // 随机指定一个队列
        if (queueIdInt < 0) {
        	/**
        	 * chen.si 设置queueId为 负数，则随机选择 分区 进行存储
        	 */
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
        }

        /**
         * chen.si 对于多tag，增加一个flag
         */
        int sysFlag = requestHeader.getSysFlag();
        // 多标签过滤需要置位
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MultiTagsFlag;
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        msgInner.setProperties(MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setPropertiesString(requestHeader.getProperties());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(topicConfig.getTopicFilterType(),
            msgInner.getTags()));

        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(sysFlag);
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());

        /**
         * chen.si TODO 了解下流程
         */
        msgInner.setReconsumeTimes(0);

        /**
         * chen.si 配置是否支持 事务消息，默认是支持
         */
        // 检查事务消息
        if (this.brokerController.getBrokerConfig().isRejectTransactionMessage()) {
            String traFlag = msgInner.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
            if (traFlag != null) {
                response.setCode(MQResponseCode.NO_PERMISSION_VALUE);
                response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                        + "] sending transaction message is forbidden");
                return response;
            }
        }

        PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        SendmsgLiveMoniter.printProcessSendmsgRequestLive(ctx.channel(), request, putMessageResult);
        if (putMessageResult != null) {
            boolean sendOK = false;

            switch (putMessageResult.getPutMessageStatus()) {
            // Success
            case PUT_OK:
                sendOK = true;
                response.setCode(ResponseCode.SUCCESS_VALUE);
                break;
            case FLUSH_DISK_TIMEOUT:
                response.setCode(MQResponseCode.FLUSH_DISK_TIMEOUT_VALUE);
                sendOK = true;
                break;
            case FLUSH_SLAVE_TIMEOUT:
                response.setCode(MQResponseCode.FLUSH_SLAVE_TIMEOUT_VALUE);
                sendOK = true;
                break;
            case SLAVE_NOT_AVAILABLE:
                response.setCode(MQResponseCode.SLAVE_NOT_AVAILABLE_VALUE);
                sendOK = true;
                break;

            // Failed
            case CREATE_MAPEDFILE_FAILED:
                response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
                response.setRemark("create maped file failed.");
                break;
            case MESSAGE_ILLEGAL:
                response.setCode(MQResponseCode.MESSAGE_ILLEGAL_VALUE);
                response.setRemark("the message is illegal, maybe length not matched.");
                break;
            case SERVICE_NOT_AVAILABLE:
                response.setCode(MQResponseCode.SERVICE_NOT_AVAILABLE_VALUE);
                response.setRemark("service not available now, maybe disk full, " + diskUtil());
                break;
            case UNKNOWN_ERROR:
                response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
                response.setRemark("UNKNOWN_ERROR");
                break;
            default:
                response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
                response.setRemark("UNKNOWN_ERROR DEFAULT");
                break;
            }

            if (sendOK) {
                response.setRemark(null);

                responseHeader.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
                responseHeader.setQueueId(queueIdInt);
                responseHeader.setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());

                /**
                 * chen.si  oneway模式，不需要返回响应；request-response模式，需要
                 */
                // 直接返回
                if (!request.isOnewayRPC()) {
                    try {
                        ctx.writeAndFlush(response).addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                                if (!future.isSuccess()) {
                                    log.error("SendMessageProcessor response to "
                                            + future.channel().remoteAddress() + " failed", future.cause());
                                    log.error(request.toString());
                                    log.error(response.toString());
                                }
                            }
                        });
                    }
                    catch (Throwable e) {
                        log.error("SendMessageProcessor process request over, but response failed", e);
                        log.error(request.toString());
                        log.error(response.toString());
                    }
                }

                /**
                 * chen.si 可能存在 被hang的 pull操作， 这里有新的消息，所以需要通知 pull操作
                 */
                this.brokerController.getPullRequestHoldService().notifyMessageArriving(
                    requestHeader.getTopic(), queueIdInt,
                    putMessageResult.getAppendMessageResult().getLogicsOffset());
                return null;
            }
        }
        else {
            response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
            response.setRemark("store putMessage return null");
        }

        return response;
    }


    public SocketAddress getStoreHost() {
        return storeHost;
    }
}
