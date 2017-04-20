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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;


/**
 * 消费队列实现
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class ConsumeQueue {
    // 存储单元大小
    public static final int CQStoreUnitSize = 20;
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    // 存储顶层对象
    private final DefaultMessageStore defaultMessageStore;
    // 存储消息索引的队列
    private final MapedFileQueue mapedFileQueue;
    // Topic
    private final String topic;
    // queueId
    private final int queueId;
    // 写索引时用到的ByteBuffer
    private final ByteBuffer byteBufferIndex;
    // 配置
    private final String storePath;
    private final int mapedFileSize;
    // 最后一个消息对应的物理Offset
    /**
     * chen.si: 最后一个逻辑消息对应的物理消息的起始Offset
     */
    private long maxPhysicOffset = -1;
    // 逻辑队列的最小Offset，删除物理文件时，计算出来的最小Offset
    // 实际使用需要除以 StoreUnitSize
    /**
     * chen.si 此队列的最小字节offset，实际上就是 第1个mapped file的第1个有效消息的global offset（第1个文件可能被特殊20字节填充的，所以是第1条有效消息）
     * 	       一般情况下就是第1个mapped file的fileoffset
     * 
     * 实际上是标识 索引队列 的1条有效消息，那说明存在无效消息，无效消息有2种：
     * 1. 填充的 特殊20字节 的消息（索引消息逻辑错乱导致）
     * 2. 对应的物理消息（即：commit log）已经被删除
     */
    private volatile long minLogicOffset = 0;


    public ConsumeQueue(//
            final String topic,//
            final int queueId,//
            final String storePath,//
            final int mapedFileSize,//
            final DefaultMessageStore defaultMessageStore) {
        this.storePath = storePath;
        this.mapedFileSize = mapedFileSize;
        this.defaultMessageStore = defaultMessageStore;

        this.topic = topic;
        this.queueId = queueId;

        String queueDir = this.storePath//
                + File.separator + topic//
                + File.separator + queueId;//

        this.mapedFileQueue = new MapedFileQueue(queueDir, mapedFileSize, null);

        this.byteBufferIndex = ByteBuffer.allocate(CQStoreUnitSize);
    }


    public boolean load() {
    	/**
    	 * chen.si 加载 <topic>/<queueid>下的 maped files
    	 */
        boolean result = this.mapedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        return result;
    }


    public void recover() {
        final List<MapedFile> mapedFiles = this.mapedFileQueue.getMapedFiles();
        if (!mapedFiles.isEmpty()) {
        	/**
        	 * chen.si 老样子，从倒数第3个开始恢复
        	 */
            // 从倒数第三个文件开始恢复
            int index = mapedFiles.size() - 3;
            if (index < 0)
                index = 0;

            int mapedFileSizeLogics = this.mapedFileSize;
            MapedFile mapedFile = mapedFiles.get(index);
            ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();
            //chen.si global offset
            long processOffset = mapedFile.getFileFromOffset();
            //chen.si local offset
            long mapedFileOffset = 0;
            while (true) {
                for (int i = 0; i < mapedFileSizeLogics; i += CQStoreUnitSize) {
                    // chen.si: 8 bytes for offset
                    long offset = byteBuffer.getLong();
                    // chen.si: 4 bytes for size
                    int size = byteBuffer.getInt();
                    // chen.si: 8 bytes for tags code
                    long tagsCode = byteBuffer.getLong();

                    // 说明当前存储单元有效
                    // TODO 这样判断有效是否合理？
                    if (offset >= 0 && size > 0) {
                    	/**
                    	 * chen.si 看是否到了最后一条消息
                    	 */
                        mapedFileOffset = i + CQStoreUnitSize;
                        /**
                         * chen.si cq的消息在commit log的最大offset
                         */
                        this.maxPhysicOffset = offset;
                    }
                    else {
                    	/**
                    	 * chen.si 文件到了最后一条消息，结束这个文件(有2种结束条件：1. 文件未写满，而结束  2. 文件写满，同时也没有写下一个文件)
                    	 * 
                    	 * 这里是第1个条件 ： 1. 文件未写满，而结束
                    	 */
                        log.info("recover current consume queue file over,  " + mapedFile.getFileName() + " "
                                + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }

                /**
                 * chen.si 只能通过local offset 和  文件满期望的 大小 来 判断  文件是否满（commit log有单独的结束标识）
                 * 			说明这个文件是因为文件满才结束的，需要继续恢复下一个文件（如果有的话）
                 * 
                 */
                // 走到文件末尾，切换至下一个文件
                if (mapedFileOffset == mapedFileSizeLogics) {
                    index++;
                    /**
                     * 这里是第2个条件 ：2. 文件写满，同时也没有写下一个文件
                     */
                    if (index >= mapedFiles.size()) {
                        // 当前条件分支不可能发生
                        log.info("recover last consume queue file over, last maped file "
                                + mapedFile.getFileName());
                        break;
                    }
                    else {
                    	/**
                    	 * chen.si 准备下一个文件
                    	 */
                        mapedFile = mapedFiles.get(index);
                        byteBuffer = mapedFile.sliceByteBuffer();
                        processOffset = mapedFile.getFileFromOffset();
                        mapedFileOffset = 0;
                        log.info("recover next consume queue file, " + mapedFile.getFileName());
                    }
                }
                else {
                    log.info("recover current consume queue queue over " + mapedFile.getFileName() + " "
                            + (processOffset + mapedFileOffset));
                    break;
                }
            }

            /**
             * 计算最大的global offset
             */
            processOffset += mapedFileOffset;
            /**
             * chen.si 为什么没有设置committedWhere？
             * 
             * processOffset实际上为最后一个有效消息的结束地址。这个地址之后的文件，都是无效的
             */
            this.mapedFileQueue.truncateDirtyFiles(processOffset);
        }
    }


    /**
     * 二分查找查找消息发送时间最接近timestamp逻辑队列的offset
     */
    public long getOffsetInQueueByTime(final long timestamp) {
        MapedFile mapedFile = this.mapedFileQueue.getMapedFileByTime(timestamp);
        if (mapedFile != null) {
            long offset = 0;
            // low:第一个索引信息的起始位置
            // minLogicOffset有设置值则从
            // minLogicOffset-mapedFile.getFileFromOffset()位置开始才是有效值
            /**
             * chen.si: 索引文件中，之前提到过，为了避免逻辑顺序错误，增加了在文件开始填充 特殊20字节，来调整 保证 索引消息位置 符合 实际位置
             * 
             * 		    所以文件开始X字节，可能不是真实的索引消息， 需要跳到第1条真实的消息：minLogicOffset-mapedFile.getFileFromOffset()
             * 
             * 		 其中，minLogicOffset是 位置的实际偏移字节量
             */
            /**
             * chen.si low为相对于文件的起始的local offset（字节偏移）
             * 
             * 目的： 找到当前maped file的第1条有效消息的位置
             * 
             * 如果minLogicOffset > mapedFile.getFileFromOffset()， 说明 这个mapedFile的刚开始部分存在 特殊20字节，所以需要去掉，不能从0计算有效消息。 
             * 其实也就是说，当前这个文件是queue的第1个文件。 否则的话，说明 这个mapedfile 不是第1个文件，就不需要考虑minLogicOffset
             */
            int low =
                    minLogicOffset > mapedFile.getFileFromOffset() ? (int) (minLogicOffset - mapedFile
                        .getFileFromOffset()) : 0;

            // high:最后一个索引信息的起始位置
            int high = 0;
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            long leftIndexValue = -1L, rightIndexValue = -1L;

            // 取出该mapedFile里面所有的映射空间(没有映射的空间并不会返回,不会返回文件空洞)
            SelectMapedBufferResult sbr = mapedFile.selectMapedBuffer(0);
            if (null != sbr) {
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                /**
                 * chen.si 最后一个索引消息的 起始 字节位置， 所以 减去 20
                 */
                high = byteBuffer.limit() - CQStoreUnitSize;
                try {
                    while (high >= low) {
                    	/**
                    	 * chen.si binary search  二分查找的基础： 索引文件中的消息，是按照store timestamp时间排序的
                    	 */
                        midOffset = (low + high) / (2 * CQStoreUnitSize) * CQStoreUnitSize;
                        byteBuffer.position(midOffset);
                        long phyOffset = byteBuffer.getLong();
                        int size = byteBuffer.getInt();

                        // 比较时间, 折半
                        /**
                         * chen.si 根据索引消息的commit phy offset和size字段，在commit log中找到这条消息的store timestamp信息
                         */
                        long storeTime =
                                this.defaultMessageStore.getCommitLog().pickupStoretimestamp(phyOffset, size);
                        if (storeTime < 0) {
                            // 没有从物理文件找到消息，此时直接返回0
                            return 0;
                        }
                        else if (storeTime == timestamp) {
                        	/**
                        	 * chen.si 正好找到，查找结束，直接break掉
                        	 */
                            targetOffset = midOffset;
                            break;
                        }
                        else if (storeTime > timestamp) {
                        	/**
                        	 * chen.si 找到了 大 的值，记下来。 再在 左边 binary search
                        	 */
                            high = midOffset - CQStoreUnitSize;
                            rightOffset = midOffset;
                            rightIndexValue = storeTime;
                        }
                        else {
                        	/**
                        	 * chen.si 找到了 小 的值，记下来。 再在 右边 binary search
                        	 */
                            low = midOffset + CQStoreUnitSize;
                            leftOffset = midOffset;
                            leftIndexValue = storeTime;
                        }
                    }

                    if (targetOffset != -1) {
                    	/**
                    	 * chen.si 找到了 相等的，之前break出来的
                    	 */
                        // 查询的时间正好是消息索引记录写入的时间
                        offset = targetOffset;
                    }
                    else {
                        if (leftIndexValue == -1) {
                            // timestamp 时间小于该MapedFile中第一条记录记录的时间
                            offset = rightOffset;
                        }
                        else if (rightIndexValue == -1) {
                            // timestamp 时间大于该MapedFile中最后一条记录记录的时间
                            offset = leftOffset;
                        }
                        else {
                            // 取最接近timestamp的offset
                            offset =
                                    Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp
                                            - rightIndexValue) ? rightOffset : leftOffset;
                        }
                    }

                    return (mapedFile.getFileFromOffset() + offset) / CQStoreUnitSize;
                }
                finally {
                    sbr.release();
                }
            }
        }

        // 映射文件被标记为不可用时返回0
        return 0;
    }


    /**
     * 根据物理Offset删除无效逻辑文件
     */
    public void truncateDirtyLogicFiles(long phyOffet) {
    	/**
    	 * chen.si phyOffset是最大commit log offset，也就是commit log下一个待写消息的位置，基于这个offset，将多余的文件删除掉
    	 */
        // 逻辑队列每个文件大小
        int logicFileSize = this.mapedFileSize;

        // 先改变逻辑队列存储的物理Offset
        /**
         * chen.si TODO 暂时不理解这里的目的
         * 
         * 再次看了一下，还是无法理解其用意，本身这个值并未在下面的逻辑中读，只有写。
         * 在任何异常情况下，也还没理解需要-1。
         *
         * 2017/04/13
         * 这里的maxPhysicOffset本质上是用来表示 consume queue中的最后一个逻辑消息对应的物理消息的physical offset
         *
         * 而参数传递进来的phyOffset，是commit log的下一个待写的位置，示意图如下：
         *
         * |msg1|msg2|msg3|msg4|......
         *                ^     ^
         *  maxPhysicOffset是第1个^的offset，指向commit log最后一条消息的起始offset
         *  phyOffset       是第2个^的offset，指向commit log下一个消息的待写offset
         *
         *  maxPhysicOffset指向的是有效的位置；而phyOffset指向的是一个无效的位置（意思是说没有实际的消息数据）
         *  为了保证maxPhysicOffset语义上的一致性，因此将maxPhysicOffset设置为phyOffset-1，指向最后一条物理消息的末尾
         *  至少是有效的，指向实际的消息数据。而且后续的恢复中，只要有一个有效消息，立刻就会将maxPhysicOffset设置为指向新的物理消息的起始offset
         *
         */
        this.maxPhysicOffset = phyOffet - 1;

        while (true) {
        	
            MapedFile mapedFile = this.mapedFileQueue.getLastMapedFile2();
            if (mapedFile != null) {
                ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();
                // 先将Offset清空
                mapedFile.setWrotePostion(0);
                mapedFile.setCommittedPosition(0);

                for (int i = 0; i < logicFileSize; i += CQStoreUnitSize) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    byteBuffer.getLong();

                    // 逻辑文件起始单元
                    if (0 == i) {
                    	/**
                    	 * chen.si 0 == i的判断，是因为 存在  删除这个文件 的可能性
                    	 */
                    	/**
                    	 * chen.si 索引文件内的第1个索引消息对应的phy offset 大于等于 传入的offset，所以直接删除掉该文件
                    	 */
                        if (offset >= phyOffet) {
                            this.mapedFileQueue.deleteLastMapedFile();
                            break;
                        }
                        else {
                        	/**
                        	 * chen.si phy offset符合条件，因此设置 相关参数
                        	 */
                            int pos = i + CQStoreUnitSize;
                            mapedFile.setWrotePostion(pos);
                            mapedFile.setCommittedPosition(pos);
                            this.maxPhysicOffset = offset;
                        }
                    }
                    // 逻辑文件中间单元
                    else {
                        // 说明当前存储单元有效
                        if (offset >= 0 && size > 0) {
                            // 如果逻辑队列存储的最大物理offset大于物理队列最大offset，则返回
                        	/**
                        	 * chen.si 不清理掉这种多余的消息？ 
                        	 * 
                        	 * 		   --即使设置了wrote pos 和 comit pos，后续写会覆盖消息，但是如果立刻正常关闭，又会如何
                        	 */
                            if (offset >= phyOffet) {
                                return;
                            }

                            int pos = i + CQStoreUnitSize;
                            mapedFile.setWrotePostion(pos);
                            mapedFile.setCommittedPosition(pos);
                            this.maxPhysicOffset = offset;

                            // 如果最后一个MapedFile扫描完，则返回
                            if (pos == logicFileSize) {
                                return;
                            }
                        }
                        else {
                        	/**
                        	 * chen.si 到了该文件的最后一条消息
                        	 */
                            return;
                        }
                    }
                }
            }
            else {
                break;
            }
        }
    }


    /**
     * 返回最后一条消息对应物理队列的Next Offset
     * 
     * chen.si 这个方法不可靠。 commit log是全局的，单从一个cq就推断出下一个 commitlog的位置，有问题。 不过没有找到 使用这个方法 的地方
     */
    public long getLastOffset() {
        // 物理队列Offset
        long lastOffset = -1;
        // 逻辑队列每个文件大小
        int logicFileSize = this.mapedFileSize;

        MapedFile mapedFile = this.mapedFileQueue.getLastMapedFile2();
        if (mapedFile != null) {
            ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();

            // 先将Offset清空
            mapedFile.setWrotePostion(0);
            mapedFile.setCommittedPosition(0);

            for (int i = 0; i < logicFileSize; i += CQStoreUnitSize) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                // 说明当前存储单元有效
                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                    int pos = i + CQStoreUnitSize;
                    mapedFile.setWrotePostion(pos);
                    mapedFile.setCommittedPosition(pos);
                    this.maxPhysicOffset = offset;
                }
                else {
                    break;
                }
            }
        }

        return lastOffset;
    }


    public boolean commit(final int flushLeastPages) {
        return this.mapedFileQueue.commit(flushLeastPages);
    }


    public int deleteExpiredFile(long offset) {
    	/**
    	 * chen.si 传入的offset是commit log的最小offset
    	 */
        int cnt = this.mapedFileQueue.deleteExpiredFileByOffset(offset, CQStoreUnitSize);
        // 无论是否删除文件，都需要纠正下最小值，因为有可能物理文件删除了，
        // 但是逻辑文件一个也删除不了
        /**
         * chen.si 2种情况，会导致minLogicOffset发生变化：
         * 
         * 1. 索引队列头 对应的几个文件，被删除掉
         * 
         * 2. 物理文件(commit log)被删除掉了，但是 索引文件 没删除掉。 为了保证有效性，同样需要correct
         */
        this.correctMinOffset(offset);
        return cnt;
    }


    /**
     * 逻辑队列的最小Offset要比传入的物理最小phyMinOffset大
     */
    public void correctMinOffset(long phyMinOffset) {
        MapedFile mapedFile = this.mapedFileQueue.getFirstMapedFileOnLock();
        if (mapedFile != null) {
            SelectMapedBufferResult result = mapedFile.selectMapedBuffer(0);
            if (result != null) {
                try {
                    // 有消息存在
                    for (int i = 0; i < result.getSize(); i += ConsumeQueue.CQStoreUnitSize) {
                        long offsetPy = result.getByteBuffer().getLong();
                        result.getByteBuffer().getInt();
                        result.getByteBuffer().getLong();

                        /**
                         * 目的： 保证索引队列的有效性（部分头消息已经是无效的，因为commit log被删除）
                         * 
                         * 基础：minLogicOffset 标识 索引队列的第1个有效消息
                         * 
                         * 因为commit log被删除了，所以 索引文件中 开始的部分索引消息 已经是 无效的了， 所以调整minLogicOffset，直到有效
                         */
                        if (offsetPy >= phyMinOffset) {
                            this.minLogicOffset = result.getMapedFile().getFileFromOffset() + i;
                            log.info("compute logics min offset: " + this.getMinOffsetInQuque() + ", topic: "
                                    + this.topic + ", queueId: " + this.queueId);
                            break;
                        }
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                finally {
                    result.release();
                }
            }
        }
    }


    public long getMinOffsetInQuque() {
        return this.minLogicOffset / CQStoreUnitSize;
    }


    public void putMessagePostionInfoWrapper(long offset, int size, long tagsCode, long storeTimestamp,
            long logicOffset) {
        final int MaxRetries = 5;
        boolean canWrite = this.defaultMessageStore.getRunningFlags().isWriteable();
        for (int i = 0; i < MaxRetries && canWrite; i++) {
        	/**
        	 * chen.si 存储索引消息
        	 */
            boolean result = this.putMessagePostionInfo(offset, size, tagsCode, logicOffset);
            if (result) {
            	/**
            	 * chen.si 设置checkpoint
            	 * 
            	 * 恢复流程 和 正常接收流程 都会更新 checkpoint
            	 */
                this.defaultMessageStore.getStoreCheckpoint().setLogicsMsgTimestamp(storeTimestamp);
                return;
            }
            // 只有一种情况会失败，创建新的MapedFile时报错或者超时
            else {
                log.warn("put commit log postion info to " + topic + ":" + queueId + " " + offset
                        + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000 * 5);
                }
                catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        this.defaultMessageStore.getRunningFlags().makeLogicsQueueError();
    }


    /**
     * 存储一个20字节的信息，putMessagePostionInfo只有一个线程调用，所以不需要加锁
     * 
     * chen.si:  
     * 
     * 			1. 存储索引消息
     * 			2. 调整潜在minlogicoffset
     * 			3. 更新maxPhyOffset
     * 
     * @param offset
     *            消息对应的CommitLog offset
     * @param size
     *            消息在CommitLog存储的大小
     * @param tagsCode
     *            tags 计算出来的长整数
     * @return 是否成功
     */
    private boolean putMessagePostionInfo(final long offset, final int size, final long tagsCode,
            final long cqOffset) {
        // 在数据恢复时会走到这个流程
    	/**
    	 * chen.si 这里是恢复的关键点，cq中的消息 有2种情况需要考虑：
    	 * 
    	 * 1. commit log中的物理消息存储成功，对应的cq的消息也存储成功，
    	 * 2. commit log中的物理消息存储成功，对应的cq的消息 未 存储成功（可能原因： broker被强制关闭等）
    	 * 
    	 * 对于第1种情况，不需要执行cq的写入操作，所以直接返回
    	 * 对于第2种情况，需要执行cq的写入操作，以 补偿 commit log中 未写入cq 的消息
    	 * 
    	 * 这里的判断依据，就是依赖 commit log中消息的phy offset  以及  cq对应的最后一条索引消息对应的 commitlog中的 phy offset
    	 * 
    	 * 如果offset <= this.maxPhysicOffset， 说明 消息在cq中是存在的
    	 * 
    	 * 否则的话，消息在cq不存在，执行写入操作。
    	 * 
    	 * 那如果中间的消息丢失，怎么办？ 不可能，commit log是同步操作的，消息是按照顺序在commit log中存储的，所以也是一条条触发给cq的
    	 */
    	
    	/**
    	 * chen.si 恢复流程走到这里，说明消息已经在cq存储成功，不需要 恢复写入，直接返回。
         *
         * 2017/04/13 异常流程下，根据checkpoint恢复时，会根据这里的物理消息的physicOffset判断，是否在逻辑队列中已经存在
         *            这里是否存在的判断，完全是根据 offset的大小进行判断的，
    	 * 
    	 * 正常接收消息写入，不会走到里面
    	 */
        if (offset <= this.maxPhysicOffset) {
            return true;
        }

        /**
         * chen.si 恢复时，如果commit log消息  未 来得及写入 cq，这里会 写
         * 
         * 正常接收消息写入，也会走这里
         */
        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQStoreUnitSize);
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
        this.byteBufferIndex.putLong(tagsCode);

        final long realLogicOffset = cqOffset * CQStoreUnitSize;

        MapedFile mapedFile = this.mapedFileQueue.getLastMapedFile(realLogicOffset);
        if (mapedFile != null) {
            // 纠正MapedFile逻辑队列索引顺序
        	/**
        	 * chen.si 所谓的逻辑队列索引顺序，比较难理解，参考如下2点：
        	 * 			1. cq中的消息是固定的20字节，所以 第几条消息在cq的位置中是固定的

						比如：第1条消息，是：0-20字节的位置；第2条消息，是：20-40字节的位置；第N条消息，是：(N-1)*20 - N*20
						
						2.  这里是假设： cq中即将写入的消息偏移（即：第几条消息）  与  当前cq文件的待写入 位置 不匹配
						
						所以认为是索引顺序有错误，进行调整，方法为：使用特殊的20字节进行填充文件开始部分，以修改cq文件的待写入位置。这里的前提是： 消息偏移 的 期望位置  比 当前位置 要小。

             2017/04/13 这里发现，所谓的调整，实际上并不是针对次序错误的调整。这里的逻辑的一个基础原则就是：只要cq中有文件，就应该是正确的。
                        唯一需要调整的是，如果cq是空的(可能是过期删除掉的)，则重建cq，而commit log中对应的消息很可能 不是 cq中的第1个消息，
                        此时则需要在新的cq文件中，直接将消息写到期望的位置，而该位置之前的内容，全部用特殊的填充消息写入
        	 */
            if (mapedFile.isFirstCreateInQueue() && cqOffset != 0 && mapedFile.getWrotePostion() == 0) {
            	/**
            	 * chen.si 符合条件：
            	 * mapedFile.isFirstCreateInQueue()  当前file queue的第1个文件
            	 * cqOffset != 0				             消息在filequeue中的offset不是0，也就是说 不是系统的第1条消息
            	 * mapedFile.getWrotePostion() == 0	   当前file创建后的第1次 写
            	 */
            	
            	/**
            	 * chen.si 设置当前在写文件的初始消息的real logic offset
            	 */
                this.minLogicOffset = realLogicOffset;
                
                /**
                 * chen.si 根据逻辑计算，找到realLogicOffset在当前文件中的实际位置。 将文件开始 到 实际位置 之间，用 特殊的20字节填充提升 wrote position
                 */
                this.fillPreBlank(mapedFile, realLogicOffset);
                log.info("fill pre blank space " + mapedFile.getFileName() + " " + realLogicOffset + " "
                        + mapedFile.getWrotePostion());
            }

            if (cqOffset != 0) {
            	/**
            	 * chen.si 尽管调整了，但是仍然存在  逻辑队列索引顺序问题。 比如 realLogicOffset滞后。此时只能告警
                 *
                 * 2017/04/13 这里其实不是调整，参考之前说的。这里的严重告警是说，发现commit log中记录的queue offset与 实际的consume queue的待写入位置不一致。
                 *
                 * 认为是一个严重的bug，原因未明。
                 *
                 * 而且后续的处理逻辑，是将错就错，继续将消息写入append到末尾。实际上这里会永远都不一致了。
                 *
                 * 2017/04/14
            	 */
                if (realLogicOffset != (mapedFile.getWrotePostion() + mapedFile.getFileFromOffset())) {
                    log.warn("logic queue order maybe wrong " + realLogicOffset + " "
                            + (mapedFile.getWrotePostion() + mapedFile.getFileFromOffset()));
                }
            }

            /**
             * chen.si 每次存储新消息，都更新maxPhysicOffset，也就是cq中对应commit log中的最大offset
             */
            // 记录物理队列最大offset
            this.maxPhysicOffset = offset;
            return mapedFile.appendMessage(this.byteBufferIndex.array());
        }

        return false;
    }


    private void fillPreBlank(final MapedFile mapedFile, final long untilWhere) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQStoreUnitSize);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        int until = (int) (untilWhere % this.mapedFileQueue.getMapedFileSize());
        for (int i = 0; i < until; i += CQStoreUnitSize) {
            mapedFile.appendMessage(byteBuffer.array());
        }
    }


    /**
     * 返回Index Buffer
     * 
     * @param startIndex
     *            起始偏移量索引
     */
    public SelectMapedBufferResult getIndexBuffer(final long startIndex) {
        int mapedFileSize = this.mapedFileSize;
        long offset = startIndex * CQStoreUnitSize;
        MapedFile mapedFile = this.mapedFileQueue.findMapedFileByOffset(offset);
        if (mapedFile != null) {
            SelectMapedBufferResult result = mapedFile.selectMapedBuffer((int) (offset % mapedFileSize));
            return result;
        }

        return null;
    }


    /**
     * chen.si 跳过 逻辑索引index 对应的文件，跳转到下一个文件，返回下一个文件的起始offset
     * 
     * @param index
     * @return
     */
    public long rollNextFile(final long index) {
        int mapedFileSize = this.mapedFileSize;
        int totalUnitsInFile = mapedFileSize / CQStoreUnitSize;
        return (index + totalUnitsInFile - index % totalUnitsInFile);
    }


    public String getTopic() {
        return topic;
    }


    public int getQueueId() {
        return queueId;
    }


    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }


    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }


    public void destroy() {
        this.maxPhysicOffset = -1;
        this.minLogicOffset = 0;
        this.mapedFileQueue.destroy();
    }


    public long getMinLogicOffset() {
        return minLogicOffset;
    }


    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }


    /**
     * 获取当前队列中的消息总数
     */
    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQuque() - this.getMinOffsetInQuque();
    }


    public long getMaxOffsetInQuque() {
        return this.mapedFileQueue.getMaxOffset() / CQStoreUnitSize;
    }
}
