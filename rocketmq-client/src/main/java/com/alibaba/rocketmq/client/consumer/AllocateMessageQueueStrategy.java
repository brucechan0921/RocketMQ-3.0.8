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
package com.alibaba.rocketmq.client.consumer;

import java.util.List;

import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * Consumer队列自动分配策略
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public interface AllocateMessageQueueStrategy {
    /**
     * 给当前的ConsumerId分配队列
     * <br/>
     * chen.si 这里的使用场景如下：
     * 			前提： 1. 一个topic对应多个分区队列  
     * 				2. 这个topic有一个consumer group进行消费
     * 				3. 有多个consumer（1个进程内多个 或者 多个进程 或者多个机器），这些consumer属于同一个consumer group
     *          在同一个group中的多个consumer中，他们需要区分的均衡的唯一的消费某个分区队列，这时候还不能多个consumer重复消费某个队列。
     *          需要一个算法，能唯一的确定均衡分配，将不同的队列分配给不同的consumer。 
     *          这个算法的难点： 必须保证不同的进程 独立的 计算属于自己的消费队列， 而且这里的消费队列不能重复
     * 
     * @param currentCID
     *            当前ConsumerId
     * @param mqAll
     *            当前Topic的所有队列集合，无重复数据，且有序
     * @param cidAll
     *            当前订阅组的所有Consumer集合，无重复数据，且有序
     * @return 分配结果，无重复数据
     */
    public List<MessageQueue> allocate(//
            final String currentCID,//
            final List<MessageQueue> mqAll,//
            final List<String> cidAll//
    );
}
