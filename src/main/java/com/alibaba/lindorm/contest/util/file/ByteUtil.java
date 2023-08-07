package com.alibaba.lindorm.contest.util.file;

public class ByteUtil {
    /**
     * int整数类型转化为4字节的byte数组
     * @param num 需要转化的整数
     * @return int值转化后的byte数组
     */
    public static byte[] intToByte(int num) {
        byte[] targets = new byte[4];
        targets[0] = (byte) (num >> 24 & 0xff);
        targets[1] = (byte) (num >> 16 & 0xff);
        targets[2] = (byte) (num >> 8 & 0xff);
        targets[3] = (byte) (num & 0xff);
        return targets;
    }

    /**
     * 字节数组转int值
     * @param bytes 字节数组
     * @param offset int值在字节数组中的偏移量
     * @return 转化后的int值
     */
    public static int byteToInt(byte[] bytes, int offset) {
        int n1 = bytes[offset] & 0xff;
        int n2 = bytes[offset + 1] & 0xff;
        int n3 = bytes[offset + 2] & 0xff;
        int n4 = bytes[offset + 3] & 0xff;
        return (n1 << 24) | (n2 << 16) | (n3 << 8) | n4;
    }

    /**
     * long整数类型转化为8字节的byte数组
     * @param num 需要转化的整数
     * @return long值转化后的byte数组
     */
    public static byte[] longToByte(long num) {
        byte[] targets = new byte[8];
        for (int i = 0; i < 8; i++) {
            int offset = (targets.length - 1 - i) * 8;
            targets[i] = (byte) ((num >>> offset) * 0xff);
        }
        return targets;
    }

    /**
     * long数组类型转化为byte数组
     * @param nums 需要转化的long数组
     * @return 转化后的byte数组
     */
    public static byte[] longArrayToByte(Long[] nums) {
        int len = nums.length;
        byte[] targets = new byte[8 * len];
        for (int i = 0; i < len; i++) {
            for (int j = 0; j < 8; j++) {
                int offset = (8 - 1 - j) * 8;
                targets[i * 8 + j] = (byte) ((nums[i] >>> offset) * 0xff);
            }
        }
        return targets;
    }
}
