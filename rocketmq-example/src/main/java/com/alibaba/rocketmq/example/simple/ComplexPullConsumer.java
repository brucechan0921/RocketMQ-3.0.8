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

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.MessageQueueListener;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * PullConsumer，订阅消息
 */
public class ComplexPullConsumer {
	private static final Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();

	public static void main(String[] args) throws MQClientException {
		DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("chensiPullConGroup");

		consumer.setMessageModel(MessageModel.CLUSTERING);
		
        MessageQueueListener listener = new MessageQueueListener() {
            @Override
            public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
                System.out.println("Topic=" + topic);
                System.out.println("mqAll:" + mqAll);
                System.out.println("mqDivided:" + mqDivided);
            }
        };
        consumer.registerMessageQueueListener("chensiTopic", listener);
        
		consumer.start();

		Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("chensiTopic");
		
		for (MessageQueue mq : mqs) {
		
			System.out.println("Consume from the queue: " + mq);
			
			try {
				PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(consumer, mq, true), 32);
				System.out.println(pullResult);
				
				putMessageQueueOffset(consumer, mq, pullResult.getNextBeginOffset());

				switch (pullResult.getPullStatus()) {
				case FOUND:
					for (MessageExt mex : pullResult.getMsgFoundList()) {
						System.out.println(mex);
					}
					break;
				case NO_MATCHED_MSG:
					break;
				case NO_NEW_MSG:
					break;
				case OFFSET_ILLEGAL:
					break;
				default:
					break;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		consumer.shutdown();
	}

	private static void putMessageQueueOffset(DefaultMQPullConsumer consumer, MessageQueue mq, long offset) throws MQClientException {
		// offseTable.put(mq, offset);
		consumer.updateConsumeOffset(mq, offset);

	}

	private static long getMessageQueueOffset(DefaultMQPullConsumer consumer,
			MessageQueue mq, boolean fromStore) throws MQClientException {
		long offset = consumer.fetchConsumeOffset(mq, fromStore);
		offset = (offset < 0 ? 0 : offset);
		System.out.println("offset:" + offset);
		return offset;
	}

	/*
	 * private static long getMessageQueueOffset(MessageQueue mq) { Long offset
	 * = offseTable.get(mq); if (offset != null) return offset;
	 * 
	 * return 0; }
	 */

}
