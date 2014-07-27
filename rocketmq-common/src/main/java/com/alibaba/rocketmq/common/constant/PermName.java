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
package com.alibaba.rocketmq.common.constant;

/**
 * chen.si broker的权限控制，基本的位操作
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class PermName {
    public static final int PERM_PRIORITY = 0x1 << 3;
    public static final int PERM_READ = 0x1 << 2;
    public static final int PERM_WRITE = 0x1 << 1;
    public static final int PERM_INHERIT = 0x1 << 0;


    public static boolean isReadable(final int perm) {
    	/**
    	 * chen.si PERM_READ位标记，是否存在
    	 */
        return (perm & PERM_READ) == PERM_READ;
    }


    public static boolean isWriteable(final int perm) {
    	/**
    	 * chen.si PERM_WRITE位标记，是否存在
    	 */
        return (perm & PERM_WRITE) == PERM_WRITE;
    }


    public static boolean isInherited(final int perm) {
    	/**
    	 * chen.si PERM_INHERIT位标记，是否存在。 这个位的意义是： 如果有继承的权限，则可以通过 此实体（比如 topic）创建 新的实体，新实体的权限继承自 原实体，但取消 继承 权限
    	 */
        return (perm & PERM_INHERIT) == PERM_INHERIT;
    }


    public static String perm2String(final int perm) {
        final StringBuffer sb = new StringBuffer("---");
        if (isReadable(perm)) {
            sb.replace(0, 1, "R");
        }

        if (isWriteable(perm)) {
            sb.replace(1, 2, "W");
        }

        if (isInherited(perm)) {
            sb.replace(2, 3, "X");
        }

        return sb.toString();
    }
}
