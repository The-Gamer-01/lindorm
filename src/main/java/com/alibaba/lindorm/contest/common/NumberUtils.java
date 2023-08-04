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

    public static int bytesToInt(byte[] buffer) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value <<= 8; // 左移8位，给下一个字节腾出位置
            value |= (buffer[i] & 0xFF); // 将下一个字节的值合并到整数中
        }
        return value;
    }

    public static byte[] intToBytes(int values) {
        byte[] buffer = new byte[4];
        for (int i = 0; i < 4; ++i) {
            int offset = 32 - (i + 1) * 8;
            buffer[i] = (byte) ((values >> offset) & 0xff);
        }
        return buffer;
    }


    public static byte[] doubleToBytes(double d) {
        long value = Double.doubleToRawLongBits(d);
        byte[] byteRet = new byte[8];
        for (int i = 0; i < 8; i++) {
            byteRet[i] = (byte) ((value >> 8 * i) & 0xff);
        }
        return byteRet;
    }

    public static double bytesToDouble(byte[] arr) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((long) (arr[i] & 0xff)) << (8 * i);
        }
        return Double.longBitsToDouble(value);
    }

    public static byte[] mergeArrays(byte[] array1, byte[] array2) {
        byte[] mergedArray = new byte[array1.length + array2.length];
        System.arraycopy(array1, 0, mergedArray, 0, array1.length);
        System.arraycopy(array2, 0, mergedArray, array1.length, array2.length);
        return mergedArray;
    }
}
