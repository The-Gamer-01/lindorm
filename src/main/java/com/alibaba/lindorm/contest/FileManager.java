package com.alibaba.lindorm.contest;

import java.io.IOException;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-02
 */
public interface FileManager {

    void write(byte[] key, byte[] value) throws IOException;

    byte[] read(byte[] key, long offset) throws IOException;

    byte[] read(byte[] key, long offset, int size) throws IOException;

    void close();

    void init(String path) throws IOException;
}
