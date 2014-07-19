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
package com.alibaba.rocketmq.broker.offset;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.ConfigManager;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * Consumer消费进度管理
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-8-11
 */
public class ConsumerOffsetManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private static final String TOPIC_GROUP_SEPARATOR = "@";

    /**
     * chen.si 消费offset有3个维度：1. topic 2. consumer group 3. queue Id
     */
    private ConcurrentHashMap<String/* topic@group */, ConcurrentHashMap<Integer, Long>> offsetTable =
            new ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>>(512);

    private transient volatile ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> offsetTableLastLast;
    private transient volatile ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> offsetTableLast;
    private transient BrokerController brokerController;


    public ConsumerOffsetManager() {
    }


    public ConsumerOffsetManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    public Set<String> whichTopicByConsumer(final String group) {
    	/**
    	 * chen.si 获取这个consumer group消费过的topic列表
    	 */
        Set<String> topics = new HashSet<String>();

        Iterator<Entry<String, ConcurrentHashMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentHashMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays != null && arrays.length == 2) {
                if (group.equals(arrays[1])) {
                    topics.add(arrays[0]);
                }
            }
        }

        return topics;
    }


    private static ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> cloneOffsetTable(
            final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> input) {
    	/**
    	 * chen.si 普通工具方法，对offset table进行clone
    	 */
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> out =
                new ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>>(input.size());

        for (String topicgroup : input.keySet()) {
            ConcurrentHashMap<Integer, Long> map = input.get(topicgroup);
            if (map != null) {
                ConcurrentHashMap<Integer, Long> mapNew = new ConcurrentHashMap<Integer, Long>(map.size());
                for (Integer queueId : map.keySet()) {
                    Long offset = map.get(queueId);
                    Integer queueIdNew = new Integer(queueId.intValue());
                    Long offsetNew = new Long(offset.longValue());
                    mapNew.put(queueIdNew, offsetNew);
                }

                String topicgroupNew = new String(topicgroup);
                out.put(topicgroupNew, mapNew);
            }
        }

        return out;
    }


    public long computePullTPS(final String topic, final String group) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        return this.computePullTPS(key);
    }


    public long computePullTPS(final String topicgroup) {
    	/**
    	 * chen.si 获取上次记录的 topicgroup 对应的offset信息
    	 */
        ConcurrentHashMap<Integer, Long> mapLast = this.offsetTableLast.get(topicgroup);
    	/**
    	 * chen.si 获取上上次记录的 topicgroup 对应的offset信息
    	 */
        ConcurrentHashMap<Integer, Long> mapLastLast = this.offsetTableLastLast.get(topicgroup);
        long totalMsgs = 0;
        if (mapLast != null && mapLastLast != null) {
            for (Integer queueIdLast : mapLast.keySet()) {
                Long offsetLast = mapLast.get(queueIdLast);
                Long offsetLastLast = mapLastLast.get(queueIdLast);
                if (offsetLast != null && offsetLastLast != null) {
                	/**
                	 * chen.si 根据queueId来找到 上次   以及 上上次  的 消费进度， 然后计算出 2次记录点 之间的消费消息总数
                	 */
                    long diff = offsetLast - offsetLastLast;
                    totalMsgs += diff;
                }
            }
        }

        if (0 == totalMsgs)
            return 0;
        /**
         * chen.si 这里的消费消息总数，认为是一个 getFlushConsumerOffsetHistoryInterval 内产生的
         * 		        为了统计TPS，也就是每秒，所以除以这个interval
         */
        double pullTps =
                totalMsgs * 1000
                        / this.brokerController.getBrokerConfig().getFlushConsumerOffsetHistoryInterval();

        return Double.valueOf(pullTps).longValue();
    }


    public void recordPullTPS() {
    	/**
    	 * chen.si 统计任务 周期性 的 调用此方法，打印 消费的 tps
    	 */
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> snapshotNow =
                cloneOffsetTable(this.offsetTable);
        this.offsetTableLastLast = this.offsetTableLast;
        this.offsetTableLast = snapshotNow;

        if (this.offsetTableLast != null && this.offsetTableLastLast != null) {
            for (String topicgroupLast : this.offsetTableLast.keySet()) {
                long tps = this.computePullTPS(topicgroupLast);
                log.info(topicgroupLast + " pull tps, " + tps);
            }
        }
    }


    public void commitOffset(final String group, final String topic, final int queueId, final long offset) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        /**
         * chen.si 更新 consumer group 对  topic和queue分区 的 消费进度
         */
        this.commitOffset(key, queueId, offset);
    }


    public long queryOffset(final String group, final String topic, final int queueId) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        /**
         * chen.si 根据consumer group、topic 和 queueId来查询 offset
         */
        ConcurrentHashMap<Integer, Long> map = this.offsetTable.get(key);
        if (null != map) {
            Long offset = map.get(queueId);
            if (offset != null)
                return offset;
        }

        return -1;
    }


    private void commitOffset(final String key, final int queueId, final long offset) {
        ConcurrentHashMap<Integer, Long> map = this.offsetTable.get(key);
        if (null == map) {
            map = new ConcurrentHashMap<Integer, Long>(32);
            map.put(queueId, offset);
            this.offsetTable.put(key, map);
        }
        else {
            map.put(queueId, offset);
        }
    }


    public String encode() {
    	/**
    	 * chen.si 序列化为字符串，便于持久化到文件
    	 */
        return this.encode(false);
    }


    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }


    @Override
    public void decode(String jsonString) {
    	/**
    	 * chen.si 字符串反序列化为 offset结构
    	 */
        if (jsonString != null) {
            ConsumerOffsetManager obj =
                    RemotingSerializable.fromJson(jsonString, ConsumerOffsetManager.class);
            if (obj != null) {
                this.offsetTable = obj.offsetTable;
            }
        }
    }


    @Override
    public String configFilePath() {
    	/**
    	 * chen.si 消费offset的文件存储路径
    	 */
        return this.brokerController.getBrokerConfig().getConsumerOffsetPath();
    }


    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> getOffsetTable() {
        return offsetTable;
    }


    public void setOffsetTable(ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> offsetTable) {
        this.offsetTable = offsetTable;
    }
}
