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
    private long maxPhysicOffset = -1;
    // 逻辑队列的最小Offset，删除物理文件时，计算出来的最小Offset
    // 实际使用需要除以 StoreUnitSize
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
        boolean result = this.mapedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        return result;
    }


    public void recover() {
        final List<MapedFile> mapedFiles = this.mapedFileQueue.getMapedFiles();
        if (!mapedFiles.isEmpty()) {
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
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    // 说明当前存储单元有效
                    // TODO 这样判断有效是否合理？
                    if (offset >= 0 && size > 0) {
                    	//cs：看是否到了最后一条消息
                        mapedFileOffset = i + CQStoreUnitSize;
                        //cs：cq的消息在commit log的最大offset
                        this.maxPhysicOffset = offset;
                    }
                    else {
                    	//chen.si 文件到了最后一条消息，结束这个文件
                        log.info("recover current consume queue file over,  " + mapedFile.getFileName() + " "
                                + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }

                //chen.si 只能通过local offset 和  文件满期望的 大小 来 判断  文件是否满（commit log有单独的结束标识）
                // 走到文件末尾，切换至下一个文件
                if (mapedFileOffset == mapedFileSizeLogics) {
                    index++;
                    if (index >= mapedFiles.size()) {
                        // 当前条件分支不可能发生
                        log.info("recover last consume queue file over, last maped file "
                                + mapedFile.getFileName());
                        break;
                    }
                    else {
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

            processOffset += mapedFileOffset;
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
    	 * chen.si phyOffset是最大offset，基于这个offset，将多余的文件删除掉
    	 */
        // 逻辑队列每个文件大小
        int logicFileSize = this.mapedFileSize;

        // 先改变逻辑队列存储的物理Offset
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
                    	 * chen.si 索引文件内的第1个索引消息对应的phy offset 大于等于 传入的offset，所以直接删除掉该文件
                    	 */
                        if (offset >= phyOffet) {
                            this.mapedFileQueue.deleteLastMapedFile();
                            break;
                        }
                        else {
                        	/**
                        	 * chen.si phy offset符合条件，因此修改 相关参数
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
        int cnt = this.mapedFileQueue.deleteExpiredFileByOffset(offset, CQStoreUnitSize);
        // 无论是否删除文件，都需要纠正下最小值，因为有可能物理文件删除了，
        // 但是逻辑文件一个也删除不了
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
            boolean result = this.putMessagePostionInfo(offset, size, tagsCode, logicOffset);
            if (result) {
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
        if (offset <= this.maxPhysicOffset) {
            return true;
        }

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
