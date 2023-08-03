package com.alibaba.lindorm.contest.common;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-02
 */
public final class HighBitPartitioner {

    /**
     * key[0] & 0xff 的结果向左移动 2 位
     * key[1] & 0xff 的结果向右移动 6 位。
     * 左移操作和右移操作的结果进行按位或操作
     * key[0] 的最低8位向左移动 2 位，与 key[1] 的最低8位向右移动 6 位进行合并, 离散性较好
     * 范围0 - 1023
     * @param key partitionKey
     * @return partitionNum
     */
    public static int getPartition(byte[] key) {
        return ((key[0] & 0xff) << 2) | ((key[1] & 0xff) >> 6);
    }
}
