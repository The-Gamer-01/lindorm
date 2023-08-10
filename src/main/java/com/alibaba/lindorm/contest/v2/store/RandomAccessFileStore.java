package com.alibaba.lindorm.contest.v2.store;

import com.alibaba.lindorm.contest.storage.MappedByteBufferWrapper;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;

import static com.alibaba.lindorm.contest.common.HighBitPartitioner.getPartition;

public class RandomAccessFileStore {

    private RandomAccessFile randomAccessFile;

    private MappedByteBuffer[] mappedByteBufferWrappers;

    public void write(byte[] key, byte[] value) throws IOException {
        /*
         *  todo
         *  1. 根据key做sharding
         *  2. 顺序刷盘
         */
    }


    public byte[] read(long offset, long size) {
        return null;
    }

    public long offset() throws IOException {
        return randomAccessFile.length();
    }
}
