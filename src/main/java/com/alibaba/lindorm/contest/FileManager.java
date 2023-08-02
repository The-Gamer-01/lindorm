package com.alibaba.lindorm.contest;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-02
 */
public interface FileManager {

    void write(byte[] key, byte[] value);

    void write(byte[] value);

    void read(byte[] key);
}
