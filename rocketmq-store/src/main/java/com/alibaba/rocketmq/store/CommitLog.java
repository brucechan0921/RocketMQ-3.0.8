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
package com.alibaba.rocketmq.store;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.store.config.BrokerRole;
import com.alibaba.rocketmq.store.config.FlushDiskType;
import com.alibaba.rocketmq.store.ha.HAService;
import com.alibaba.rocketmq.store.schedule.ScheduleMessageService;


/**
 * CommitLog实现
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class CommitLog {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    // 每个消息对应的MAGIC CODE daa320a7
    /**
     * chen.si 标识commit log的普通消息
     */
    private final static int MessageMagicCode = 0xAABBCCDD ^ 1880681586 + 8;
    // 文件末尾空洞对应的MAGIC CODE cbd43194
    /**
     * chen.si 标识commit log的文件末尾
     */
    private final static int BlankMagicCode = 0xBBCCDDEE ^ 1880681586 + 8;
    // 存储消息的队列
    private final MapedFileQueue mapedFileQueue;
    // 存储顶层对象
    private final DefaultMessageStore defaultMessageStore;
    // CommitLog刷盘服务
    private final FlushCommitLogService flushCommitLogService;
    // 存储消息时的回调接口
    private final AppendMessageCallback appendMessageCallback;
    // 用来保存每个ConsumeQueue的当前最大Offset信息
    private HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable = new HashMap<String, Long>(
        1024);


    /**
     * 构造函数
     */
    public CommitLog(final DefaultMessageStore defaultMessageStore) {
    	/**
    	 * chen.si 初始化存储数据消息的抽象队列
    	 */
        this.mapedFileQueue =
                new MapedFileQueue(defaultMessageStore.getMessageStoreConfig().getStorePathCommitLog(),
                    defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog(),
                    defaultMessageStore.getAllocateMapedFileService());
        this.defaultMessageStore = defaultMessageStore;

        /**
         * chen.si 刷盘类型，可配置
         */
        if (FlushDiskType.SYNC_FLUSH == defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            this.flushCommitLogService = new GroupCommitService();
        }
        else {
            this.flushCommitLogService = new FlushRealTimeService();
        }

        this.appendMessageCallback =
                new DefaultAppendMessageCallback(defaultMessageStore.getMessageStoreConfig()
                    .getMaxMessageSize());
    }


    public boolean load() {
        boolean result = this.mapedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }


    /**
     * chen.si 启动刷盘服务
     */
    public void start() {
        this.flushCommitLogService.start();
    }

    /**
     * chen.si 关闭刷盘服务
     */
    public void shutdown() {
        this.flushCommitLogService.shutdown();
    }


    public long getMinOffset() {
        MapedFile mapedFile = this.mapedFileQueue.getFirstMapedFileOnLock();
        if (mapedFile != null) {
        	/**
        	 * chen.si TODO
        	 */
            if (mapedFile.isAvailable()) {
                return mapedFile.getFileFromOffset();
            }
            else {
            	/**
            	 * chen.si 找到下一个文件的offset
            	 */
                return this.rollNextFile(mapedFile.getFileFromOffset());
            }
        }

        return -1;
    }


    /**
     * chen.si 找到当前offset对应的下一个文件，返回下一个文件的起始offset
     * 			注意：这里的下一个文件的起始offset可能仍然是不可用的
     * @param offset
     * @return
     */
    public long rollNextFile(final long offset) {
        int mapedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        return (offset + mapedFileSize - offset % mapedFileSize);
    }


    /**
     * chen.si 获取commit log的最大的offset，即：队列尾的offset，此offset不指向任何消息，指向下一个待写的位置
     * @return
     */
    public long getMaxOffset() {
        return this.mapedFileQueue.getMaxOffset();
    }


    /**
     * chen.si 用于 commit log文件的自动清理服务， 消息最多保留N天，超过N天的，必须删除掉，判断依据为file的元数据 最近修改时间戳
     * 
     * @param expiredTime
     * @param deleteFilesInterval
     * @param intervalForcibly
     * @param cleanImmediately
     * @return
     */
    public int deleteExpiredFile(//
            final long expiredTime, //
            final int deleteFilesInterval, //
            final long intervalForcibly,//
            final boolean cleanImmediately//
    ) {
        return this.mapedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval,
            intervalForcibly, cleanImmediately);
    }


    /**
     * 读取CommitLog数据，数据复制时使用
     */
    public SelectMapedBufferResult getData(final long offset) {
    	/**
    	 * chen.si  用0 表示 queue的第1个文件
    	 */
        return this.getData(offset, (0 == offset ? true : false));
    }


    public SelectMapedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        int mapedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        MapedFile mapedFile = this.mapedFileQueue.findMapedFileByOffset(offset, returnFirstOnNotFound);
        if (mapedFile != null) {
            int pos = (int) (offset % mapedFileSize);
            SelectMapedBufferResult result = mapedFile.selectMapedBuffer(pos);
            return result;
        }

        return null;
    }


    /**
     * 正常退出时，数据恢复，所有内存数据都已经刷盘
     */
    public void recoverNormally() {
    	/**
    	 * chen.si：这个方法的主要任务如下：
    	 * 
    	 * 1. 设置 commit log queue中的最后一个消息位置
    	 * 
    	 * 2. 设置最后一个消息所在 log文件的commit 和 write position
    	 * 
    	 * 3. 删除多余的文件
    	 */
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MapedFile> mapedFiles = this.mapedFileQueue.getMapedFiles();
        if (!mapedFiles.isEmpty()) {
        	/**
        	 * chen.si:正常情况下，3个文件足够恢复了。也就是说，最后一个可写文件一般就在这3个文件中
        	 */
            // 从倒数第三个文件开始恢复
            int index = mapedFiles.size() - 3;
            if (index < 0)
                index = 0;

            MapedFile mapedFile = mapedFiles.get(index);
            ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();
            /**
             * chen.si：这个是个global offset，指示最后一条消息
             */
            long processOffset = mapedFile.getFileFromOffset();
            /**
             * chen.si:当前文件内的offset
             */
            long mapedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest =
                        this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();
                // 正常数据
                if (size > 0) {
                	/**
                	 * chen.si:累加当前文件的消息offset(local offset  另外一个称为global offset)
                	 */
                    mapedFileOffset += size;
                }
                // 文件中间读到错误
                /**
                 * chen.si：文件未写完而已，并不是错误。此时直接break，因为这个文件就是最后一个待用文件
                 */
                else if (size == -1) { //new DispatchRequest(-1)
                    log.info("recover physics file end, " + mapedFile.getFileName());
                    break;
                }
                // 走到文件末尾，切换至下一个文件
                // 由于返回0代表是遇到了最后的空洞，这个可以不计入truncate offset中
                else if (size == 0) { //new DispatchRequest(0)
                	/**
                	 * chen.si：遇到了空洞文件末尾，切换到下一个文件
                	 */
                    index++;
                    if (index >= mapedFiles.size()) {
                        // 当前条件分支不可能发生
                        log.info("recover last 3 physics file over, last maped file "
                                + mapedFile.getFileName());
                        break;
                    }
                    else {
                        mapedFile = mapedFiles.get(index);
                        byteBuffer = mapedFile.sliceByteBuffer();
                        /**
                         * chen.si:每个map file的起始global offset，
                         */
                        processOffset = mapedFile.getFileFromOffset();
                        /**
                         * chen.si:localoffset
                         */
                        mapedFileOffset = 0;
                        log.info("recover next physics file, " + mapedFile.getFileName());
                    }
                }
            }

            processOffset += mapedFileOffset;
            // chen.si：最后一个消息的global offset
            this.mapedFileQueue.setCommittedWhere(processOffset);
            // chen.si：删除无用文件，其实就是 到 当前ready to write的文件 为止，后续的文件，都属于dirty files
            this.mapedFileQueue.truncateDirtyFiles(processOffset);
        }
    }


    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }


    /**
     * 服务端使用 检查消息并返回消息大小
     * 
     * @return 0 表示走到文件末尾 >0 正常消息 -1 消息校验失败
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
            final boolean readBody) {
        try {
            java.nio.ByteBuffer byteBufferMessage =
                    ((DefaultAppendMessageCallback) this.appendMessageCallback).getMsgStoreItemMemory();
            byte[] bytesContent = byteBufferMessage.array();

            // 1 TOTALSIZE
            int totalSize = byteBuffer.getInt();

            // 2 MAGICCODE
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
            case MessageMagicCode:
                break;
            case BlankMagicCode:
                return new DispatchRequest(0);
            default:
            	/**
            	 * chen.si：找到了最后一条消息，并且文件未写完，仍然可写。所以这里是一个常态，非异常
            	 */
                log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                return new DispatchRequest(-1);
            }

            // 3 BODYCRC
            int bodyCRC = byteBuffer.getInt();

            // 4 QUEUEID
            int queueId = byteBuffer.getInt();

            // 5 FLAG
            int flag = byteBuffer.getInt();
            flag = flag + 0;

            // 6 QUEUEOFFSET
            long queueOffset = byteBuffer.getLong();

            // 7 PHYSICALOFFSET
            long physicOffset = byteBuffer.getLong();

            // 8 SYSFLAG
            int sysFlag = byteBuffer.getInt();

            // 9 BORNTIMESTAMP
            long bornTimeStamp = byteBuffer.getLong();
            bornTimeStamp = bornTimeStamp + 0;

            // 10 BORNHOST（IP+PORT）
            byteBuffer.get(bytesContent, 0, 8);

            // 11 STORETIMESTAMP
            long storeTimestamp = byteBuffer.getLong();

            // 12 STOREHOST（IP+PORT）
            byteBuffer.get(bytesContent, 0, 8);

            // 13 RECONSUMETIMES
            int reconsumeTimes = byteBuffer.getInt();

            // 14 Prepared Transaction Offset
            long preparedTransactionOffset = byteBuffer.getLong();

            // 15 BODY
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byteBuffer.get(bytesContent, 0, bodyLen);

                    // 校验CRC
                    if (checkCRC) {
                        int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            log.warn("CRC check failed " + crc + " " + bodyCRC);
                            return new DispatchRequest(-1);
                        }
                    }
                }
                else {
                	//chen.si：不需要body，直接跳过
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            // 16 TOPIC
            byte topicLen = byteBuffer.get();
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen);

            long tagsCode = 0;
            String keys = "";

            // 17 properties
            short propertiesLength = byteBuffer.getShort();
            if (propertiesLength > 0) {
                byteBuffer.get(bytesContent, 0, propertiesLength);
                String properties = new String(bytesContent, 0, propertiesLength);
                Map<String, String> propertiesMap = MessageDecoder.string2messageProperties(properties);

                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);
                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                if (tags != null && tags.length() > 0) {
                    tagsCode =
                            MessageExtBrokerInner.tagsString2tagsCode(
                                MessageExt.parseTopicFilterType(sysFlag), tags);
                }
            }

            return new DispatchRequest(//
                topic,// 1
                queueId,// 2
                physicOffset,// 3
                totalSize,// 4
                tagsCode,// 5
                storeTimestamp,// 6
                queueOffset,// 7
                keys,// 8
                sysFlag,// 9
                0L,// 10
                preparedTransactionOffset,// 11
                null// 12
            );
        }
        catch (BufferUnderflowException e) {
            byteBuffer.position(byteBuffer.limit());
        }
        catch (Exception e) {
            byteBuffer.position(byteBuffer.limit());
        }

        return new DispatchRequest(-1);
    }


    public void recoverAbnormally() {
        // 根据最小时间戳来恢复
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MapedFile> mapedFiles = this.mapedFileQueue.getMapedFiles();
        if (!mapedFiles.isEmpty()) {
            // 寻找从哪个文件开始恢复
            int index = mapedFiles.size() - 1;
            MapedFile mapedFile = null;
            for (; index >= 0; index--) {
                mapedFile = mapedFiles.get(index);
                /**
                 * chen.si 不是恢复所有文件checkpoint。看如下的示意图
                 * |F1| |F2| |F3|
                 * checkpoint是一个时间戳，实际上可能会指向任意一个文件的任意位置，一般来说是最后1个文件，因为checkpoint一直在刷盘。
                 * 具体指向什么，是按照commit log的消息的storeTimestamp来确定的。确定方式，就是比较checkpoint时间戳和File的第1个消息的storeTimestamp
                 * 如果checkpoint的时间戳 晚于 消息的storeTimestamp，则说明就从这个文件开始恢复，否则继续找前一个文件
                 *
                 */
                if (this.isMapedFileMatchedRecover(mapedFile)) {
                	
                	// 考虑 store时间戳的误差，所以从上一个文件进行恢复，防止消息丢失
                	// TODO 最准确的方式是直接找到上一个文件的checkpoint对应的点，然后恢复剩余的消息，避免恢复整个文件。
                	//      但是commit log需要从头寻找才能确定消息，而且都要走一遍page cache，性能相差基本不大
                    log.info("recover from this maped file " + mapedFile.getFileName());
                    break;
                }
                /**
                 * chen.si 走到这里，说明 需要找 再之前的文件， 进行恢复
                 */
            }

            if (index < 0) {
                index = 0;
                mapedFile = mapedFiles.get(index);
            }

            ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();
            /**
             * chen.si global offset
             */
            long processOffset = mapedFile.getFileFromOffset();
            /**
             * chen.si local offset
             */
            long mapedFileOffset = 0;
            while (true) {
            	/**
            	 * chen.si 深入看checkMessageAndReturnSize方法，会发现最后DispatchRequest的producerGroup为null
            	 * 
            	 * 这是因为：关于事务消息 的恢复，有单独的恢复流程，直接根据cq来恢复
            	 */
                DispatchRequest dispatchRequest =
                        this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();
                // 正常数据
                if (size > 0) {
                    mapedFileOffset += size;
                    /**
                     * chen.si 关键点，重放 commit log的消息 到 cq中， cq会选择是 因为已经建立索引消息而忽略， 或者 补偿建立索引消息
                     */
                    this.defaultMessageStore.putDispatchRequest(dispatchRequest);
                }
                // 文件中间读到错误
                else if (size == -1) {
                    log.info("recover physics file end, " + mapedFile.getFileName());
                    break;
                }
                // 走到文件末尾，切换至下一个文件
                // 由于返回0代表是遇到了最后的空洞，这个可以不计入truncate offset中
                else if (size == 0) {
                    index++;
                    if (index >= mapedFiles.size()) {
                        // 当前条件分支正常情况下不应该发生
                        log.info("recover physics file over, last maped file " + mapedFile.getFileName());
                        break;
                    }
                    else {
                        mapedFile = mapedFiles.get(index);
                        byteBuffer = mapedFile.sliceByteBuffer();
                        processOffset = mapedFile.getFileFromOffset();
                        mapedFileOffset = 0;
                        log.info("recover next physics file, " + mapedFile.getFileName());
                    }
                }
            }

            processOffset += mapedFileOffset;
            this.mapedFileQueue.setCommittedWhere(processOffset);
            this.mapedFileQueue.truncateDirtyFiles(processOffset);

            // 清除ConsumeQueue的多余数据
            /**
             * chen.si TODO 不知道什么场景会产生如下情况：
             * 
             * cq的索引消息中存在多余消息，其 对应的 数据消息 在 commit log中不存在。 
             * 
             * 这个是以文件为级别的，发现中的第1条消息是多余消息，则删掉整个文件。
             * 如果文件中 开始一部分消息合法，但是 后续消息不合法呢？cq.truncateDirtyLogicFiles方法中没看到处理逻辑，却是直接return的
             *
             * 这里并没有覆盖文件后续的内容，而是直接通过write和commit position来截止到这里，实际上可能是不安全的。
             * 因为假设此时不再有新的消息，但是正常关闭了，则下次cq被恢复时，会直接全部读完。
             *
             * TODO 建议这里覆盖掉后面的内容
             *
             * 2017/04/13
             * 实际上这里有一个问题，就是上面的this.defaultMessageStore.putDispatchRequest(dispatchRequest);是异步的
             * 这里直接truncate，是不正确的，需要等待上述恢复完成，这里才能truncate
             *
             * 好在，3.5.8甚至后面的4.0版本，已经全部采用同步的方式
             *
             */
            this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
        }
        // 物理文件都被删除情况下
        else {
        	/**
        	 * chen.si 直接重置cq
        	 */
            this.mapedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }


    private boolean isMapedFileMatchedRecover(final MapedFile mapedFile) {
        ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();

        /**
         * chen.si 找第1个物理消息
         */
        int magicCode = byteBuffer.getInt(MessageDecoder.MessageMagicCodePostion);
        if (magicCode != MessageMagicCode) {
            return false;
        }

        /**
         * chen.si 获取第1个物理消息的存储时间
         */
        long storeTimestamp = byteBuffer.getLong(MessageDecoder.MessageStoreTimestampPostion);
        if (0 == storeTimestamp) {
            return false;
        }

        /**
         * chen.si 带上 index queue的 时间 来比较
         */
        if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()//
                && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                log.info("find check timestamp, {} {}", //
                    storeTimestamp,//
                    UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        }
        else {
        	/**
        	 * chen.si 直接比较  commit queue 和 cq 的时间  
        	 */
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                log.info("find check timestamp, {} {}", //
                    storeTimestamp,//
                    UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        }

        return false;
    }


    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {
    	/**
    	 * chen.si 真正存储消息的入口
    	 */
        // 设置存储时间
        msg.setStoreTimestamp(System.currentTimeMillis());
        // 设置消息体BODY CRC（考虑在客户端设置最合适）
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        // 返回结果
        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        String topic = msg.getTopic();
        int queueId = msg.getQueueId();
        long tagsCode = msg.getTagsCode();

        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        if (tranType == MessageSysFlag.TransactionNotType//
                || tranType == MessageSysFlag.TransactionCommitType) {
        	/**
        	 * chen.si 对于普通消息 和 commit消息，需要考虑 延迟发送 功能
        	 */
            // 延时投递
            if (msg.getDelayTimeLevel() > 0) {
                if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService()
                    .getMaxDelayLevel()) {
                    msg.setDelayTimeLevel(this.defaultMessageStore.getScheduleMessageService()
                        .getMaxDelayLevel());
                }

                topic = ScheduleMessageService.SCHEDULE_TOPIC;
                queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());
                tagsCode =
                        this.defaultMessageStore.getScheduleMessageService().computeDeliverTimestamp(
                            msg.getDelayTimeLevel(), msg.getStoreTimestamp());

                /**
                 * 备份真实的topic，queueId
                 */
                msg.putProperty(MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
                msg.putProperty(MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

                msg.setTopic(topic);
                msg.setQueueId(queueId);
            }
        }

        // 写文件要加锁
        synchronized (this) {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();

            // 这里设置存储时间戳，才能保证全局有序
            /**
             * chen.si 这个是关键点， 才能保证后续的 恢复流程，可以依赖 存储时间戳
             * 
             * 不过这里的SystemClock是定时更新的，1ms更新一次。 
             * 实际上可能会出现多个消息的store时间一致的情况，会导致异常恢复，如果同一时间的消息跨越2个文件，会导致消息漏恢复
             */
            msg.setStoreTimestamp(beginLockTimestamp);

            // 尝试写入
            MapedFile mapedFile = this.mapedFileQueue.getLastMapedFile();
            if (null == mapedFile) {
                log.error("create maped file1 error, topic: " + msg.getTopic() + " clientAddr: "
                        + msg.getBornHostString());
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }
            result = mapedFile.appendMessage(msg, this.appendMessageCallback);
            switch (result.getStatus()) {
            // 成功追加消息
            case PUT_OK:
                break;
            // 走到文件末尾
            case END_OF_FILE:
                // 创建新文件，重新写消息
                mapedFile = this.mapedFileQueue.getLastMapedFile();
                if (null == mapedFile) {
                    log.error("create maped file2 error, topic: " + msg.getTopic() + " clientAddr: "
                            + msg.getBornHostString());
                    return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                }
                result = mapedFile.appendMessage(msg, this.appendMessageCallback);
                break;
            // 消息大小超限
            case MESSAGE_SIZE_EXCEEDED:
                return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                // 未知错误
            case UNKNOWN_ERROR:
                return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            default:
                return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            /**
             * chen.si:这里的cq 和 tran消息都是异步，在commit log成功后，系统宕机，会导致消息直接丢失。 所以有异常恢复机制来确保消息不丢
             */
            DispatchRequest dispatchRequest = new DispatchRequest(//
                topic,// 1
                queueId,// 2
                result.getWroteOffset(),// 3
                result.getWroteBytes(),// 4
                tagsCode,// 5
                msg.getStoreTimestamp(),// 6
                result.getLogicsOffset(),// 7
                msg.getKeys(),// 8
                /**
                 * 事务部分
                 */
                msg.getSysFlag(),// 9
                msg.getQueueOffset(), // 10
                msg.getPreparedTransactionOffset(),// 11
                msg.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP)// 12
                    );

            this.defaultMessageStore.putDispatchRequest(dispatchRequest);

            long eclipseTime = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            if (eclipseTime > 1000) {
                log.warn("putMessage in lock eclipse time(ms) " + eclipseTime);
            }
        }

        // 返回结果
        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // 统计消息SIZE
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(result.getWroteBytes());

        GroupCommitRequest request = null;

        // 同步刷盘
        if (FlushDiskType.SYNC_FLUSH == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
        	/**
        	 * chen.si 同步模式， 将消息发送给 flush 线程， flush成功后，才会返回，除非超时
        	 */
            GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
            if (msg.isWaitStoreMsgOK()) {
                request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                service.putRequest(request);
                boolean flushOK =
                        request.waitForFlush(this.defaultMessageStore.getMessageStoreConfig()
                            .getSyncFlushTimeout());
                if (!flushOK) {
                    log.error("do groupcommit, wait for flush failed, topic: " + msg.getTopic() + " tags: "
                            + msg.getTags() + " client address: " + msg.getBornHostString());
                    putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }
            }
            else {
                service.wakeup();
            }
        }
        // 异步刷盘
        else {
        	/**
        	 * chen.si 通知进行刷盘
        	 */
            this.flushCommitLogService.wakeup();
        }

        // 同步双写
        if (BrokerRole.SYNC_MASTER == this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
            HAService service = this.defaultMessageStore.getHaService();
            if (msg.isWaitStoreMsgOK()) {
                // 判断是否要等待
                if (service.isSlaveOK(result.getWroteOffset() + result.getWroteBytes())) {
                    if (null == request) {
                        request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                    }
                    service.putRequest(request);

                    service.getWaitNotifyObject().wakeupAll();

                    boolean flushOK =
                    // TODO 此处参数与刷盘公用是否合适
                            request.waitForFlush(this.defaultMessageStore.getMessageStoreConfig()
                                .getSyncFlushTimeout());
                    if (!flushOK) {
                        log.error("do sync transfer other node, wait return, but failed, topic: "
                                + msg.getTopic() + " tags: " + msg.getTags() + " client address: "
                                + msg.getBornHostString());
                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }
                }
                // Slave异常
                else {
                    // 告诉发送方，Slave异常
                    putMessageResult.setPutMessageStatus(PutMessageStatus.SLAVE_NOT_AVAILABLE);
                }
            }
        }

        // 向发送方返回结果
        return putMessageResult;
    }


    /**
     * 根据offset获取特定消息的存储时间 如果出错，则返回-1
     */
    public long pickupStoretimestamp(final long offset, final int size) {
    	/**
    	 * chen.si offset为phy offset
    	 */
        SelectMapedBufferResult result = this.getMessage(offset, size);
        if (null != result) {
            try {
                return result.getByteBuffer().getLong(MessageDecoder.MessageStoreTimestampPostion);
            }
            finally {
                result.release();
            }
        }

        return -1;
    }


    /**
     * 读取消息
     */
    public SelectMapedBufferResult getMessage(final long offset, final int size) {
        int mapedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        /**
         * chen.si 获取phy offset所在的map file
         */
        MapedFile mapedFile = this.mapedFileQueue.findMapedFileByOffset(offset, (0 == offset ? true : false));
        if (mapedFile != null) {
        	/**
        	 * chen.si 获取指定位置的消息的消息缓冲区
        	 */
            int pos = (int) (offset % mapedFileSize);
            SelectMapedBufferResult result = mapedFile.selectMapedBuffer(pos, size);
            return result;
        }

        return null;
    }


    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }


    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }


    public void destroy() {
        this.mapedFileQueue.destroy();
    }


    public boolean appendData(long startOffset, byte[] data) {
        // 写文件要加锁
        synchronized (this) {
            // 尝试写入
            MapedFile mapedFile = this.mapedFileQueue.getLastMapedFile(startOffset);
            if (null == mapedFile) {
                log.error("appendData getLastMapedFile error  " + startOffset);
                return false;
            }

            return mapedFile.appendMessage(data);
        }
    }


    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mapedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }

    abstract class FlushCommitLogService extends ServiceThread {
    }

    /**
     * 异步实时刷盘服务
     */
    class FlushRealTimeService extends FlushCommitLogService {
        private static final int RetryTimesOver = 3;
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;


        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                int interval =
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig()
                            .getFlushIntervalCommitLog();
                int flushPhysicQueueLeastPages =
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig()
                            .getFlushCommitLogLeastPages();

                int flushPhysicQueueThoroughInterval =
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig()
                            .getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // 定时刷盘，定时打印刷盘进度
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = ((printTimes++ % 10) == 0);
                }

                try {
                    this.waitForRunning(interval);

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    /**
                     * chen.si 3种情况，会触发实际的commit：
                     * 
                     * 1. 超时时间到了，必须刷新
                     * 2. 超时时间未到，但是数据已经超过X页，必须刷新（尽管waitForRunning每次都会被唤醒，并且执行commit，但是commit中会忽略不满X页的commit）
                     * 3. 超时时间未到，数据未超过X页，但是 文件满了
                     * 
                     * 以上3种情况，只会commit当前文件，如果还存在下一个文件（最后一个文件，即：queue.committedWhere指向倒数第2个文件），
                     * 则下一个文件的数据必须等到下一次commit
                     */
                    CommitLog.this.mapedFileQueue.commit(flushPhysicQueueLeastPages);
                    /**
                     * chens.si TODO 第2和3种情况的刷新，不更新storeTimestamp，也不会更新checkpoint， 不知道为什么
                     * 
                     * 至少不会造成错误，checkpoint比实际操作慢一点，是没有问题的
                     */
                    long storeTimestamp = CommitLog.this.mapedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(
                            storeTimestamp);
                    }
                }
                catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // 正常shutdown时，要保证全部刷盘才退出
            /**
             * chen.si TODO 最后要再看下，是不是commit log完成了，才会触发这里。 不然的话，会有问题（文件满的情况下造成问题）
             */
            boolean result = false;
            for (int i = 0; i < RetryTimesOver && !result; i++) {
                result = CommitLog.this.mapedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times "
                        + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            CommitLog.log.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return FlushCommitLogService.class.getSimpleName();
        }


        private void printFlushProgress() {
            CommitLog.log.info("how much disk fall behind memory, "
                    + CommitLog.this.mapedFileQueue.howMuchFallBehind());
        }


        @Override
        public long getJointime() {
            // 由于CommitLog数据量较大，所以回收时间要更长
            return 1000 * 60 * 5;
        }
    }

    public class GroupCommitRequest {
        // 当前消息对应的下一个Offset
        private final long nextOffset;
        // 异步通知对象
        private final CountDownLatch countDownLatch = new CountDownLatch(1);
        // 刷盘是否成功
        private volatile boolean flushOK = false;


        public GroupCommitRequest(long nextOffset) {
            this.nextOffset = nextOffset;
        }


        public long getNextOffset() {
            return nextOffset;
        }


        public void wakeupCustomer(final boolean flushOK) {
            this.flushOK = flushOK;
            this.countDownLatch.countDown();
        }


        public boolean waitForFlush(long timeout) {
            try {
                boolean result = this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
                return result || this.flushOK;
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        }
    }

    /**
     * GroupCommit Service
     */
    class GroupCommitService extends FlushCommitLogService {
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<GroupCommitRequest>();
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<GroupCommitRequest>();


        public void putRequest(final GroupCommitRequest request) {
            synchronized (this) {
                this.requestsWrite.add(request);
                if (!this.hasNotified) {
                    this.hasNotified = true;
                    this.notify();
                }
            }
        }


        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }


        private void doCommit() {
            if (!this.requestsRead.isEmpty()) {
                for (GroupCommitRequest req : this.requestsRead) {
                	// 消息有可能在下一个文件，所以最多刷盘2次
                	/**
                	 * chen.si 如果上一个文件满，则先要刷上一个文件，然后再刷当前文件。所以可能要循环2次，具体参考commit方法
                	 */
                    boolean flushOK = false;
                    for (int i = 0; (i < 2) && !flushOK; i++) {
                    	/**
                    	 * chen.si queue中当前消息是否已经刷新到磁盘
                    	 */
                        flushOK = (CommitLog.this.mapedFileQueue.getCommittedWhere() >= req.getNextOffset());

                        if (!flushOK) {
                        	/**
                        	 * chen.si 强制刷新
                        	 */
                            CommitLog.this.mapedFileQueue.commit(0);
                        }
                    }

                    /**
                     * chen.si 唤醒commit log线程，数据消息存储成功
                     */
                    req.wakeupCustomer(flushOK);
                }

                long storeTimestamp = CommitLog.this.mapedFileQueue.getStoreTimestamp();
                /**
                 * chen.si 刷新成功，设置commit log的checkpoint
                 * 
                 * TODO 单看这里的时间，是有问题的。 commit完成后， store的时间可能已经变化了，这样就标识的checkpoint 比实际的 要大
                 * 除非 commit那边是同步的， 这边也是单线程的，再看看异步模式
                 */
                if (storeTimestamp > 0) {
                    CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(
                        storeTimestamp);
                }

                this.requestsRead.clear();
            }
            else {
                // 由于个别消息设置为不同步刷盘，所以会走到此流程
                CommitLog.this.mapedFileQueue.commit(0);
            }
        }


        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    this.waitForRunning(0);
                    this.doCommit();
                }
                catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // 在正常shutdown情况下，等待请求到来，然后再刷盘
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }


        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }


        @Override
        public String getServiceName() {
            return GroupCommitService.class.getSimpleName();
        }


        @Override
        public long getJointime() {
            // 由于CommitLog数据量较大，所以回收时间要更长
            return 1000 * 60 * 5;
        }
    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // 文件末尾空洞最小定长
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        // 存储消息ID
        private final ByteBuffer msgIdMemory;
        // 存储消息内容
        private final ByteBuffer msgStoreItemMemory;
        // 消息的最大长度
        private final int maxMessageSize;


        DefaultAppendMessageCallback(final int size) {
            this.msgIdMemory = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
            this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
            this.maxMessageSize = size;
        }


        public ByteBuffer getMsgStoreItemMemory() {
            return msgStoreItemMemory;
        }


        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
                final int maxBlank, final Object msg) {
            /**
             * 生成消息ID STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>
             */
            MessageExtBrokerInner msgInner = (MessageExtBrokerInner) msg;
            /**
             * chen.si 这里单独传参fileFromOffset，理解不了用意。
             * 			 对于commit log来说，只负责持久化消息，它面向的应该是 一个 buffer，以及  标识当前位置的global offset 
             */
            // PHY OFFSET
            long wroteOffset = fileFromOffset + byteBuffer.position();
            String msgId =
                    MessageDecoder.createMessageId(this.msgIdMemory, msgInner.getStoreHostBytes(),
                        wroteOffset);

            /**
             * 记录ConsumeQueue信息
             */
            String key = msgInner.getTopic() + "-" + msgInner.getQueueId();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }

            /**
             * 事务消息需要特殊处理
             */
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
            /**
             * chen.si prepare消息是新消息，所以需要offset，指向transaction table
             */
            case MessageSysFlag.TransactionPreparedType:
                queueOffset =
                        CommitLog.this.defaultMessageStore.getTransactionStateService()
                            .getTranStateTableOffset().get();
                break;
            case MessageSysFlag.TransactionRollbackType:
                queueOffset = msgInner.getQueueOffset();
                break;
            case MessageSysFlag.TransactionNotType:
            case MessageSysFlag.TransactionCommitType:
            default:
                break;
            }

            /**
             * 序列化消息
             */
            final byte[] propertiesData =
                    msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes();
            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

            final byte[] topicData = msgInner.getTopic().getBytes();
            final int topicLength = topicData == null ? 0 : topicData.length;

            final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

            final int msgLen = 4 // 1 TOTALSIZE
                    + 4 // 2 MAGICCODE
                    + 4 // 3 BODYCRC
                    + 4 // 4 QUEUEID
                    + 4 // 5 FLAG
                    + 8 // 6 QUEUEOFFSET
                    + 8 // 7 PHYSICALOFFSET
                    + 4 // 8 SYSFLAG
                    + 8 // 9 BORNTIMESTAMP
                    + 8 // 10 BORNHOST
                    + 8 // 11 STORETIMESTAMP
                    + 8 // 12 STOREHOSTADDRESS
                    + 4 // 13 RECONSUMETIMES
                    + 8 // 14 Prepared Transaction Offset
                    + 4 + bodyLength // 14 BODY
                    + 1 + topicLength // 15 TOPIC
                    + 2 + propertiesLength // 16 propertiesLength
                    + 0;

            // 消息超过设定的最大值
            if (msgLen > this.maxMessageSize) {
                CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: "
                        + bodyLength + ", maxMessageSize: " + this.maxMessageSize);
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }

            // 判断是否有足够空余空间
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.resetMsgStoreItemMemory(maxBlank);
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(CommitLog.BlankMagicCode);
                // 3 剩余空间可能是任何值
                //

                // 此处长度特意设置为maxBlank
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId,
                    msgInner.getStoreTimestamp(), queueOffset);
            }

            // 初始化存储空间
            this.resetMsgStoreItemMemory(msgLen);
            // 1 TOTALSIZE
            this.msgStoreItemMemory.putInt(msgLen);
            // 2 MAGICCODE
            this.msgStoreItemMemory.putInt(CommitLog.MessageMagicCode);
            // 3 BODYCRC
            this.msgStoreItemMemory.putInt(msgInner.getBodyCRC());
            // 4 QUEUEID
            this.msgStoreItemMemory.putInt(msgInner.getQueueId());
            // 5 FLAG
            this.msgStoreItemMemory.putInt(msgInner.getFlag());
            // 6 QUEUEOFFSET
            this.msgStoreItemMemory.putLong(queueOffset);
            // 7 PHYSICALOFFSET
            this.msgStoreItemMemory.putLong(fileFromOffset + byteBuffer.position());
            // 8 SYSFLAG
            this.msgStoreItemMemory.putInt(msgInner.getSysFlag());
            // 9 BORNTIMESTAMP
            this.msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
            // 10 BORNHOST
            this.msgStoreItemMemory.put(msgInner.getBornHostBytes());
            // 11 STORETIMESTAMP
            this.msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
            // 12 STOREHOSTADDRESS
            this.msgStoreItemMemory.put(msgInner.getStoreHostBytes());
            // 13 RECONSUMETIMES
            this.msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
            // 14 Prepared Transaction Offset
            this.msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
            // 15 BODY
            this.msgStoreItemMemory.putInt(bodyLength);
            if (bodyLength > 0)
                this.msgStoreItemMemory.put(msgInner.getBody());
            // 16 TOPIC
            this.msgStoreItemMemory.put((byte) topicLength);
            this.msgStoreItemMemory.put(topicData);
            // 17 PROPERTIES
            this.msgStoreItemMemory.putShort((short) propertiesLength);
            if (propertiesLength > 0)
                this.msgStoreItemMemory.put(propertiesData);

            // 向队列缓冲区写入消息
            byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);

            AppendMessageResult result =
                    new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgId,
                        msgInner.getStoreTimestamp(), queueOffset);

            switch (tranType) {
            case MessageSysFlag.TransactionPreparedType:
                CommitLog.this.defaultMessageStore.getTransactionStateService().getTranStateTableOffset()
                    .incrementAndGet();
                break;
            case MessageSysFlag.TransactionRollbackType:
                break;
            case MessageSysFlag.TransactionNotType:
            case MessageSysFlag.TransactionCommitType:
                // 更新下一次的ConsumeQueue信息
                CommitLog.this.topicQueueTable.put(key, ++queueOffset);
                break;
            default:
                break;
            }

            // 返回结果
            return result;
        }


        private void resetMsgStoreItemMemory(final int length) {
            this.msgStoreItemMemory.flip();
            this.msgStoreItemMemory.limit(length);
        }
    }
}
