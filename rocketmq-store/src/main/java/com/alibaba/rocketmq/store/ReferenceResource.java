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

import java.util.concurrent.atomic.AtomicLong;


/**
 * 引用计数基类，类似于C++智能指针实现
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public abstract class ReferenceResource {
    /**
     * chen.si 默认值是1，也就是文件被mmap后，就说明有1次引用，除非被shutdown，才会变为0
     */
    protected final AtomicLong refCount = new AtomicLong(1);
    protected volatile boolean available = true;
    protected volatile boolean cleanupOver = false;
    private volatile long firstShutdownTimestamp = 0;


    /**
     * 资源是否能HOLD住
     */
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            /**
             * chen.si: 将文件的ref +1，这里的ref主要用于计算当前有多少对该资源的引用，实际上就是对MapedFile的引用。
             *
             * 实际上ref说明，当前资源正在被使用
             *
             * 这里的引用包括3点：
             * <p>
             * 1. MapedFile自身的使用，比如commit之前，先hold，commit完成后，然后release</p>
             * <p>
             * 2. SelectMapedBufferResult的使用，创建时需要先hold，等buffer用完后，需要release。
             * 因为这里都是MappedFileBuffer，SelectMapedBufferResult内的buffer指向文件，如果MapedFile不可用了，buffer也就没有了。
             * 所以，对于SelectMapedBufferResult来说，需要增加ref</p>
             * <p>
             * 3.  IndexFile的使用
             * </p>
             */
            if (this.refCount.getAndIncrement() > 0) {
                // chen.si +1后，如果>0，说明可用
                return true;
            }
            else {
                // chen.si +1后，仍然<=0，说明资源不可用，将之前的+1回滚，也就是-1
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }


    /**
     * 资源是否可用，即是否可被HOLD
     */
    public boolean isAvailable() {
        return this.available;
    }


    /**
     * 禁止资源被访问 shutdown不允许调用多次，最好是由管理线程调用
     */
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        }
        // 强制shutdown
        else if (this.getRefCount() > 0) {
            /**
             * chen.si 超过X秒，资源仍然还被引用，则强制将ref设置为负数，
             */
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                /**
                 * chen.si 超时了，直接强制将ref设置为-(1000 + ref)，这样子可以确保ref为负数
                 * 猜测是防止还有人在申请hold这个资源，进行+1操作，最终导致永远关闭不了。所以弄个-1000。实际上，设置个最大负数更干脆
                 */
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }


    public long getRefCount() {
        return this.refCount.get();
    }


    /**
     * 释放资源
     */
    public void release() {
        /**
         * chen.si 调用此方法，则减少1次ref。
         * <p>正常情况下，SelectMapedBufferResult会产生新的引用，正常也会release掉，最终ref会等于默认值1</p>
         * <p>所以在MapedFile调用shutdown时，这里就会变为0，说明是想彻底释放资源，包括删除文件或者shutdown进程。
         * 因此这里最终可以彻底释放资源，关闭MapedFile，unmmap这个文件。
         * </p>
         */
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;

        synchronized (this) {
            // cleanup内部要对是否clean做处理
            this.cleanupOver = this.cleanup(value);
        }
    }


    /**
     * chen.si 清理资源，也就是关闭MapedFile，取消mmap
     * @param currentRef
     * @return
     */
    public abstract boolean cleanup(final long currentRef);


    /**
     * 资源是否被清理完成
     */
    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
