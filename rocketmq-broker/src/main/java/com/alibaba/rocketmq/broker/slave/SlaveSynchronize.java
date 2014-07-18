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
package com.alibaba.rocketmq.broker.slave;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.subscription.SubscriptionGroupManager;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.body.ConsumerOffsetSerializeWrapper;
import com.alibaba.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import com.alibaba.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;


/**
 * Slave从Master同步信息（非消息）
 * 
 * chen.si 包括4类信息：  消费进度、定时进度、topic配置 和 sub group信息
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @author manhong.yqd<manhong.yqd@taobao.com>
 * @since 2013-7-8
 */
public class SlaveSynchronize {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private final BrokerController brokerController;
    /**
     * chen.si master的地址，slave通过这个地址对应的master中 拉取 相关元数据和进度信息
     */
    private volatile String masterAddr = null;


    public SlaveSynchronize(BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    public String getMasterAddr() {
        return masterAddr;
    }


    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }


    public void syncAll() {
    	/**
    	 * chen.si 同步其他的进度信息
    	 * 			
    	 * 			物理消息 通过HaService同步；其他的信息，通过这里同步
    	 */
        this.syncTopicConfig();
        this.syncConsumerOffset();
        this.syncDelayOffset();
        this.syncSubscriptionGroupConfig();
    }


    private void syncTopicConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
            	/**
            	 * chen.si 拉取topic信息
            	 */
                TopicConfigSerializeWrapper topicWrapper =
                        this.brokerController.getBrokerOuterAPI().getAllTopicConfig(masterAddrBak);
                
                /**
                 * chen.si 比较版本，因为topic一般的在线改动不大，所以增加个 版本，必须重复的同步
                 */
                if (!this.brokerController.getTopicConfigManager().getDataVersion()
                    .equals(topicWrapper.getDataVersion())) {

                	/**
                	 * chen.si 更新topic信息
                	 */
                    this.brokerController.getTopicConfigManager().getDataVersion()
                        .assignNewOne(topicWrapper.getDataVersion());
                    this.brokerController.getTopicConfigManager().getTopicConfigTable().clear();
                    this.brokerController.getTopicConfigManager().getTopicConfigTable()
                        .putAll(topicWrapper.getTopicConfigTable());
                    /**
                     * chen.si 持久化到文件
                     */
                    this.brokerController.getTopicConfigManager().persist();

                    log.info("update slave topic config from master, {}", masterAddrBak);
                }
            }
            catch (Exception e) {
                log.error("syncTopicConfig Exception, " + masterAddrBak, e);
            }
        }
    }


    private void syncConsumerOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
            	/**
            	 * chen.si 拉取消费进度信息
            	 */
                ConsumerOffsetSerializeWrapper offsetWrapper =
                        this.brokerController.getBrokerOuterAPI().getAllConsumerOffset(masterAddrBak);
                /**
                 * chen.si 更新消费进度信息
                 */
                this.brokerController.getConsumerOffsetManager().getOffsetTable()
                    .putAll(offsetWrapper.getOffsetTable());
                /**
                 * chen.si 持久化到文件
                 */
                this.brokerController.getConsumerOffsetManager().persist();
                log.info("update slave consumer offset from master, {}", masterAddrBak);
            }
            catch (Exception e) {
                log.error("syncConsumerOffset Exception, " + masterAddrBak, e);
            }
        }
    }


    private void syncDelayOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
            	/**
            	 * chen.si 拉取定时消息处理进度信息
            	 */
                String delayOffset =
                        this.brokerController.getBrokerOuterAPI().getAllDelayOffset(masterAddrBak);
                if (delayOffset != null) {
                    /**
                     * chen.si 持久化到文件
                     */
                    String fileName = this.brokerController.getMessageStoreConfig().getDelayOffsetStorePath();
                    try {
                        MixAll.string2File(delayOffset, fileName);
                    }
                    catch (IOException e) {
                        log.error("persist file Exception, " + fileName, e);
                    }
                }
                log.info("update slave delay offset from master, {}", masterAddrBak);
            }
            catch (Exception e) {
                log.error("syncDelayOffset Exception, " + masterAddrBak, e);
            }
        }
    }


    private void syncSubscriptionGroupConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
                /**
                 * chen.si TODO 拉取 SubscriptionGroup 信息
                 */
                SubscriptionGroupWrapper subscriptionWrapper =
                        this.brokerController.getBrokerOuterAPI()
                            .getAllSubscriptionGroupConfig(masterAddrBak);

                /**
                 * chen.si 比较版本
                 */
                if (!this.brokerController.getSubscriptionGroupManager().getDataVersion()
                    .equals(subscriptionWrapper.getDataVersion())) {
                	/**
                	 * chen.si 更新信息
                	 */
                    SubscriptionGroupManager subscriptionGroupManager =
                            this.brokerController.getSubscriptionGroupManager();
                    subscriptionGroupManager.getDataVersion().assignNewOne(
                        subscriptionWrapper.getDataVersion());
                    subscriptionGroupManager.getSubscriptionGroupTable().clear();
                    subscriptionGroupManager.getSubscriptionGroupTable().putAll(
                        subscriptionWrapper.getSubscriptionGroupTable());
                    
                    /**
                     * chen.si 更新到文件
                     */
                    subscriptionGroupManager.persist();
                    log.info("update slave Subscription Group from master, {}", masterAddrBak);
                }
            }
            catch (Exception e) {
                log.error("syncSubscriptionGroup Exception, " + masterAddrBak, e);
            }
        }
    }
}
