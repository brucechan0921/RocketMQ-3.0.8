package com.alibaba.rocketmq.example.simple;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;

public class PullMsgFromMqTask implements Runnable {

	private final DefaultMQPullConsumer consumer;
	private final MessageQueue partition;
	private AtomicBoolean isOK;
	private final CountDownLatch stopLatch;

	public PullMsgFromMqTask(DefaultMQPullConsumer consumer, MessageQueue partition, AtomicBoolean isOK,
			CountDownLatch stopLatch) {
		this.consumer = consumer;
		this.partition = partition;
		this.isOK = isOK;
		this.stopLatch = stopLatch;
	}

	@Override
	public void run() {

		while (this.isOK.get()) {
			System.out.println("Consume from the queue: " + partition);

			try {
				// 获取上一次的消费进度
				long nextOffset = getMessageQueueOffset(consumer, partition, true);

				for (;;) {
					// 根据消费进度，获取下一批消息
					PullResult pullResult = consumer.pullBlockIfNotFound(partition, null, nextOffset, 32);

					switch (pullResult.getPullStatus()) {
					case FOUND:
						// 处理消息列表
						int index = 0;
						
						for (MessageExt msg : pullResult.getMsgFoundList()) {
							index++;
							boolean isMsgFailed = false;
							System.out.println(msg);
							
							// mock
							if (index % 2 ==0) { 
								isMsgFailed = true; 
							}
							// the message failed, need to send back to wait for retry.
							if (isMsgFailed) {
								this.consumer.sendMessageBack(msg, 2);
							}
						}
						// 处理完成，让broker保存消费进度
						putMessageQueueOffset(consumer, partition, pullResult.getNextBeginOffset());
						nextOffset = pullResult.getNextBeginOffset();

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
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// current task is ready to stop
		this.stopLatch.countDown();

		System.out.println("notified to stop");
	}

	private static void putMessageQueueOffset(DefaultMQPullConsumer consumer, MessageQueue mq, long offset)
			throws MQClientException {
		consumer.updateConsumeOffset(mq, offset);
	}

	private static long getMessageQueueOffset(DefaultMQPullConsumer consumer, MessageQueue mq, boolean fromStore)
			throws MQClientException {
		long offset = consumer.fetchConsumeOffset(mq, fromStore);
		offset = (offset < 0 ? 0 : offset);
		System.out.println("offset:" + offset);
		return offset;
	}
}
