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
package com.alibaba.rocketmq.store.schedule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.ConfigManager;
import com.alibaba.rocketmq.common.TopicFilterType;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.running.RunningStats;
import com.alibaba.rocketmq.store.ConsumeQueue;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.MessageExtBrokerInner;
import com.alibaba.rocketmq.store.PutMessageResult;
import com.alibaba.rocketmq.store.PutMessageStatus;
import com.alibaba.rocketmq.store.SelectMapedBufferResult;


/**
 * 定时消息服务
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class ScheduleMessageService extends ConfigManager {
	/**
	 * chen.si 定时消息，继续采用cq的模式，使用 固定的topic名称，其中每个delayLeve 对应一个 topic的分区
	 */
    public static final String SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;
    // 每个level对应的延时时间
    private final ConcurrentHashMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
            new ConcurrentHashMap<Integer, Long>(32);
    // 延时计算到了哪里
    private final ConcurrentHashMap<Integer /* level */, Long/* offset */> offsetTable =
            new ConcurrentHashMap<Integer, Long>(32);
    // 定时器
    private final Timer timer = new Timer("ScheduleMessageTimerThread", true);
    // 存储顶层对象
    private final DefaultMessageStore defaultMessageStore;
    // 最大值
    private int maxDelayLevel;


    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }


    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQuque(SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }


    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }


    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }


    private void updateOffset(int delayLevel, long offset) {
    	/**
    	 * chen.si 更新offset的 缓存
    	 */
        this.offsetTable.put(delayLevel, offset);
    }


    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
    	/**
    	 * chen.si 根据deplayLevel 计算出 真实的消息延迟发送绝对时间
    	 */
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }


    public void start() {
        // 为每个延时队列增加定时器
        for (Integer level : this.delayLevelTable.keySet()) {
            Long timeDelay = this.delayLevelTable.get(level);
            Long offset = this.offsetTable.get(level);
            if (null == offset) {
                offset = 0L;
            }

            if (timeDelay != null) {
                this.timer.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME);
            }
        }

        // 定时将延时进度刷盘
        this.timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                try {
                    ScheduleMessageService.this.persist();
                }
                catch (Exception e) {
                    log.error("scheduleAtFixedRate flush exception", e);
                }
            }
        }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
    }


    public void shutdown() {
        this.timer.cancel();
    }


    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }


    public String encode() {
        return this.encode(false);
    }


    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }


    @Override
    public void decode(String jsonString) {
    	/**
    	 * chen.si 加载 定时处理进度 文件
    	 */
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                    DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }


    @Override
    public String configFilePath() {
    	/**
    	 * chen.si 定时处理进度 文件： store\config\delayOffset.json
    	 */
        return this.defaultMessageStore.getMessageStoreConfig().getDelayOffsetStorePath();
    }


    public boolean load() {
        boolean result = super.load();
        result = result && this.parseDelayLevel();
        return result;
    }


    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                long delayTimeMillis = tu * num;
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        }
        catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    class DeliverDelayedMessageTimerTask extends TimerTask {
        private final int delayLevel;
        /**
         * chen.si queue的logic offset
         */
        private final long offset;


        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }


        @Override
        public void run() {
            try {
                this.executeOnTimeup();
            }
            catch (Exception e) {
                log.error("executeOnTimeup exception", e);
                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                    this.delayLevel, this.offset), DELAY_FOR_A_PERIOD);
            }
        }


        public void executeOnTimeup() {
        	/**
        	 * chen.si 获取 delayLevel 对应的 consume queue
        	 */
            ConsumeQueue cq =
                    ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(SCHEDULE_TOPIC,
                        delayLevel2QueueId(delayLevel));
            if (cq != null) {
            	/**
            	 * chen.si 从 指定的 位置 开始，寻找待发送消息
            	 */
                SelectMapedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
                if (bufferCQ != null) {
                    try {
                    	/**
                    	 * chen.si 记录 定时队列 的处理进度
                    	 */
                        long nextOffset = offset;
                        int i = 0;
                        /**
                         * chen.si: https://github.com/alibaba/RocketMQ/issues/470
                         * 
                         *  定时服务 处理 cq中的定时消息时，将当前文件的可用缓冲区 的 到期消息 一次全部 写入commit log，才会更新offset。
							假设这样的场景：极端情况下，整个文件的到期消息都写入commit log完成，但是此时宕机，offset没来得及更新，最终整个文件的到期消息会全部被重新处理写入commit log一遍。
							在大量使用定时消息时，这样造成的消息重复量太大。
							
							建议增加如下功能：
								cq的到期消息一次批量处理 超过X条，立刻更新offset
								超过Y条到期消息 被处理， 也触发 定时处理进度写磁盘 的操作（目前是Y秒会写一次，Y可配置）
                         */
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQStoreUnitSize) {
                        	/**
                        	 * chen.si 定时消息的3个索引信息
                        	 */
                            long offsetPy = bufferCQ.getByteBuffer().getLong();
                            int sizePy = bufferCQ.getByteBuffer().getInt();
                            long tagsCode = bufferCQ.getByteBuffer().getLong();

                            // 队列里存储的tagsCode实际是一个时间点
                            long deliverTimestamp = tagsCode;

                            /**
                             * chen.si 计算下一个定时消息的位置
                             */
                            nextOffset = offset + (i / ConsumeQueue.CQStoreUnitSize);

                            /**
                             * chen.si 是否到期了
                             */
                            long countdown = deliverTimestamp - System.currentTimeMillis();
                            // 时间到了，该投递
                            if (countdown <= 0) {
                            	/**
                            	 * chen.si 从commit log中找到定时的数据消息
                            	 */
                                MessageExt msgExt =
                                        ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(
                                            offsetPy, sizePy);
                                if (msgExt != null) {
                                	/**
                                	 * chen.si 重新构建  到期的 数据消息
                                	 */
                                    MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                                    /**
                                     * chen.si 作为普通消息，放入commit log
                                     */
                                    PutMessageResult putMessageResult =
                                            ScheduleMessageService.this.defaultMessageStore
                                                .putMessage(msgInner);
                                    // 成功
                                    if (putMessageResult != null
                                            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                    	/**
                                    	 * chen.si 继续读取文件，尝试下一条消息
                                    	 */
                                        continue;
                                    }
                                    // 失败
                                    else {
                                    	/**
                                    	 * chen.si 当前到期的消息 处理失败，只能跳过忽略。 进行下一条消息的处理
                                    	 */
                                        log.error(
                                            "a message time up, but reput it failed, topic: {} msgId {}",
                                            msgExt.getTopic(), msgExt.getMsgId());
                                        
                                        /**
                                         * chen.si TODO 这个重新启动timer，为什么间隔10s这么长，会导致消息不及时被处理吧
                                         */
                                        ScheduleMessageService.this.timer.schedule(
                                            new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset),
                                            DELAY_FOR_A_PERIOD);
                                        /**
                                         * chen.si 更新当前定时队列的 处理进度
                                         */
                                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                        return;
                                    }
                                }
                            }
                            // 时候未到，继续定时
                            else {
                            	/**
                            	 * chen.si 精确控制，只等待  剩余的超时间隔
                            	 */
                                ScheduleMessageService.this.timer.schedule(
                                    new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset),
                                    countdown);
                                /**
                                 * chen.si 更新当前定时队列的 处理进度
                                 */
                                ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                return;
                            }
                        } // end of for

                        /**
                         * chen.si 当前的定时消息缓冲 处理结束，后续从nextOff接着处理
                         */
                        nextOffset = offset + (i / ConsumeQueue.CQStoreUnitSize);
                        ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                            this.delayLevel, nextOffset), DELAY_FOR_A_WHILE);
                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    }
                    finally {
                        // 必须释放资源
                        bufferCQ.release();
                    }
                } // end of if (bufferCQ != null)
            } // end of if (cq != null)

            /**
             * chen.si 如果cq  或者  buffer 未生成，则 下一次再检查
             */
            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel,
                this.offset), DELAY_FOR_A_WHILE);
        }


        private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            msgInner.setProperties(msgExt.getProperties());

            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            long tagsCodeValue =
                    MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            msgInner.setWaitStoreMsgOK(false);
            /**
             * chen.si 已经到期，需要作为普通消息进行处理，去除  定时  的属性
             */
            msgInner.clearProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL);

            /**
             * chen.si 借助定时队列 的 topic 和 queueId，来记录定时消息。   到期后，需要恢复topic和queueId，准备重新放入commit log，作为普通消息处理
             */
            // 恢复Topic
            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

            // 恢复QueueId
            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }
    }
}
