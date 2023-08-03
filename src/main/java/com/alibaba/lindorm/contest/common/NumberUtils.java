package com.alibaba.lindorm.contest.common;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-03
 */
public class NumberUtils {

    public static long bytesToLong(byte[] buffer) {
        long values = 0;
        int len = 8;
        for (int i = 0; i < len; ++i) {
            values <<= 8;
            values |= (buffer[i] & 0xff);
        }
        return values;
    }

    public static byte[] longTobytes(long values) {
        byte[] buffer = new byte[8];
        for (int i = 0; i < 8; ++i) {
            int offset = 64 - (i + 1) * 8;
            buffer[i] = (byte) ((values >> offset) & 0xff);
        }
        return buffer;
    }
}
