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
package com.alibaba.rocketmq.broker.client;

import io.netty.channel.Channel;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;


/**
 * Consumer连接、订阅关系管理
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class ConsumerManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private final ConcurrentHashMap<String/* Group */, ConsumerGroupInfo> consumerTable =
            new ConcurrentHashMap<String, ConsumerGroupInfo>(1024);

    private final ConsumerIdsChangeListener consumerIdsChangeListener;
    private static final long ChannelExpiredTimeout = 1000 * 120;


    public ConsumerManager(final ConsumerIdsChangeListener consumerIdsChangeListener) {
        this.consumerIdsChangeListener = consumerIdsChangeListener;
    }


    public ConsumerGroupInfo getConsumerGroupInfo(final String group) {
        return this.consumerTable.get(group);
    }


    public SubscriptionData findSubscriptionData(final String group, final String topic) {
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.findSubscriptionData(topic);
        }

        return null;
    }


    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        for (String group : this.consumerTable.keySet()) {
            final ConsumerGroupInfo info = this.consumerTable.get(group);
            if (info != null) {
                info.doChannelCloseEvent(remoteAddr, channel);
                this.consumerIdsChangeListener.consumerIdsChanged(group, info.getAllChannel());
            }
        }
    }


    /**
     * 返回是否有变化
     */
    public boolean registerConsumer(final String group, final ClientChannelInfo clientChannelInfo,
            ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
            final Set<SubscriptionData> subList) {
        /*
        chen.si 记录 订阅组的信息,订阅组包括：
        1. 消费组组名
        2. 订阅的主题和tags
        3. 消费类型（就是指消费者是以PUSH还是PULL的方式获取消息）
        4. 消费模型（广播/集群）
        5. 从哪里开始消费

         */
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null == consumerGroupInfo) {
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere);
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(group, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }

        /*
        chen.si 如果是新的连接，则说明有新的consumer来了，更新到 消费组 里面
         */
        boolean r1 =
                consumerGroupInfo.updateChannel(clientChannelInfo, consumeType, messageModel,
                    consumeFromWhere);

        /*
        chen.si 订阅关系发生了变化
         */
        boolean r2 = consumerGroupInfo.updateSubscription(subList);

        if (r1 || r2) {
            /*
            chen.si 通知消费组对应的所有消费者，需要重新进行负载均衡

            2017/05/10 这里存在一个问题，如果通知给消费者失败，怎么办？

            没有问题，消费端会周期性拉取订阅信息，进行本地更新。在这个更新周期内，可能会有消息重复。
             */
            this.consumerIdsChangeListener.consumerIdsChanged(group, consumerGroupInfo.getAllChannel());
        }

        return r1 || r2;
    }


    public void unregisterConsumer(final String group, final ClientChannelInfo clientChannelInfo) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null != consumerGroupInfo) {
            consumerGroupInfo.unregisterChannel(clientChannelInfo);
            this.consumerIdsChangeListener.consumerIdsChanged(group, consumerGroupInfo.getAllChannel());
        }
    }


    public void scanNotActiveChannel() {
        for (String group : this.consumerTable.keySet()) {
            final ConsumerGroupInfo info = this.consumerTable.get(group);
            if (info != null) {
                ConcurrentHashMap<Channel, ClientChannelInfo> cloneChannels =
                        new ConcurrentHashMap(info.getChannelInfoTable());
                for (Map.Entry<Channel, ClientChannelInfo> entry : cloneChannels.entrySet()) {
                    ClientChannelInfo clientChannelInfo = entry.getValue();
                    long diff = System.currentTimeMillis() - clientChannelInfo.getLastUpdateTimestamp();
                    if (diff > ChannelExpiredTimeout) {
                        log.warn(
                            "SCAN: remove expired channel from ConsumerManager consumerTable. channel={}, consumerGroup={}",
                            RemotingHelper.parseChannelRemoteAddr(entry.getKey()), group);
                        RemotingUtil.closeChannel(clientChannelInfo.getChannel());
                        info.getChannelInfoTable().remove(entry.getKey());
                    }
                }
            }
        }
    }
}
