package com.alibaba.lindorm.contest.storage;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-04
 */
public interface Compresser {

    byte[] compress(byte[] data);

    byte[] decompress(byte[] data);
}
