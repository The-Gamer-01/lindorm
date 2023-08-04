package com.alibaba.lindorm.contest.storage;

import static com.alibaba.lindorm.contest.common.HighBitPartitioner.getPartition;

import java.io.File;
import java.io.IOException;
import java.util.stream.Stream;

import com.alibaba.lindorm.contest.FileManager;
import com.alibaba.lindorm.contest.common.HighBitPartitioner;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-02
 */
public class FileStorage implements FileManager {

    private static final int PARTITION_NUM = 1 << 10;

    private MappedByteBufferWrapper[] mappedByteBufferWrappers;

    @Override
    public void init(String path) throws IOException {
        mappedByteBufferWrappers = new MappedByteBufferWrapper[PARTITION_NUM];
        for (int i = 0; i < PARTITION_NUM; i++) {
            mappedByteBufferWrappers[i] = new MappedByteBufferWrapper();
            mappedByteBufferWrappers[i].init(path, i);
        }
    }


    @Override
    public void write(byte[] key, byte[] value) throws IOException {
        int partitionKey = getPartition(key);
        MappedByteBufferWrapper wrapper = mappedByteBufferWrappers[partitionKey];
        if (wrapper == null) {
            throw new RuntimeException("wrapper is nonnull");
        }
        wrapper.write(value);
    }

    @Override
    public byte[] read(byte[] key, long offset) throws IOException {
        int partitionKey = getPartition(key);
        MappedByteBufferWrapper wrapper = mappedByteBufferWrappers[partitionKey];
        if (wrapper == null) {
            throw new RuntimeException("wrapper is nonnull");
        }
        return wrapper.read(offset);
    }

    @Override
    public void close() {

    }

    public MappedByteBufferWrapper[] getMappedByteBufferWrappers() {
        return mappedByteBufferWrappers;
    }
}
