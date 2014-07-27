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
package com.alibaba.rocketmq.example.simple;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.MessageQueueListener;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * PullConsumer，订阅消息
 */
public class ComplexPullConsumer {

	public static void main(String[] args) throws MQClientException, InterruptedException {
		final String consumerGroup = "testPullConGroup";
		DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(consumerGroup);
		//设置为集群模式
		consumer.setMessageModel(MessageModel.CLUSTERING);
		
		/*
		 * TODO 这个还不行，直接使用wait/notify 可能会导致 错过notify
		 */
		final Object balanceChanged = new Object(); 
		
        MessageQueueListener listener = new MessageQueueListener() {
            @Override
            public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            	/*
            	 * chen.si 根据queue的变化，通知consumer的fetch queue，重新pull
            	 */
                System.out.println("Topic=" + topic);
                System.out.println("mqAll:" + mqAll);
                System.out.println("mqDivided:" + mqDivided);
                
                balanceChanged.notifyAll();
            }
        };
        final String topic = "testTopic";
		final String retryTopic = MixAll.getRetryTopic(consumerGroup);

        consumer.registerMessageQueueListener(topic, listener);
        consumer.registerMessageQueueListener(retryTopic, listener);
        
		consumer.start();

		ExecutorService consumerService = Executors.newCachedThreadPool();
		consumerService.submit(new ScheduleTopicTask(consumer, topic, Executors.newCachedThreadPool(), balanceChanged));
		consumerService.submit(new ScheduleTopicTask(consumer, retryTopic, Executors.newCachedThreadPool(), balanceChanged));

		Thread.sleep(Long.MAX_VALUE);
		
		consumer.shutdown();
	}
}
