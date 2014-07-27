package com.alibaba.rocketmq.example.simple;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageQueue;

public class ScheduleTopicTask implements Runnable {

	private final DefaultMQPullConsumer consumer;
	private final String topic;
	private final ExecutorService topicService;
	private final Object balanceChanged;

	public ScheduleTopicTask(DefaultMQPullConsumer consumer, String topic, ExecutorService topicService, Object balanceChanged) {
		this.topic = topic;
		this.consumer = consumer;
		this.topicService = topicService;
		this.balanceChanged = balanceChanged;
	}

	@Override
	public void run() {

		System.out.println("Begin to handle topic request: " + topic);
		for (;;) {
			// 使用这种方式，让多个consumer在多个分区之间进行负载均衡消费
			// 订阅topic分区
			CountDownLatch stopLatch = null;
			AtomicBoolean mqTaskRunning = null;
			
			boolean isErrorHappen = false;
			try {
				System.out.println("Begin to fetch mqs for topic : " + topic);
				Set<MessageQueue> topicMqs = consumer.fetchMessageQueuesInBalance(topic);

				System.out.println("finished to fetch mqs for topic : " + topic + " " + topicMqs);
				
				if (topicMqs.size() > 0) {
					stopLatch = new CountDownLatch(topicMqs.size());
					mqTaskRunning = new AtomicBoolean(true);

					/*
					 * TODO 这里不好，如果submit内部出错，会导致 一部分成功，一部分失败。最好避免 资源泄露
					 */
					for (MessageQueue mq : topicMqs) {
						System.out.println("Begin to submit mq pull task for topic : " + topic + " " + mq);
						topicService.submit(new PullMsgFromMqTask(consumer, mq, mqTaskRunning, stopLatch));
					}
				} else {
					// wait for a while to retry to get the mqs
					isErrorHappen = true;
				}
			} catch (MQClientException e) {
				isErrorHappen = true;
				e.printStackTrace();
			}
			
			if (!isErrorHappen) {
				// wait for the notification that some changes happen for the queues.
				// need to rebalance the queues.
				try {
					System.out.println("Begin to wait mqs changed for topic : " + topic);
					this.balanceChanged.wait();
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				
				System.out.println("found mqs changed for topic : " + topic);

				// 1. stops the mq task
				mqTaskRunning.set(false);
				// 2. wait for the stopping, TODO use timeout
				try {
					stopLatch.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
			} else {
				// error happen, wait and retry
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
		}
	}

}
