package com.alibaba.lindorm.contest.storage;

import static com.alibaba.lindorm.contest.common.Preconditions.checkFileState;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-02
 */
public class MappedByteBufferWrapper {

    private static ThreadLocal<ByteBuffer> bufferThreadLocal =
            ThreadLocal.withInitial(() -> ByteBuffer.allocate(4 * 1024));

    private RandomAccessFile randomAccessFile;

    private MappedByteBuffer mappedByteBuffer;

    private AtomicLong wrotePosition;

    public void init(String path, int n) throws IOException {
        File dir = new File(path);
        if (!dir.exists()) {
            checkFileState(dir.mkdirs(), "创建文件夹失败，path: " + path);
        }
        File file = new File(path + File.separator + n);
        if (!file.exists()) {
            checkFileState(file.createNewFile(), "创建文件失败, path: " + path);
        }
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.mappedByteBuffer = randomAccessFile.getChannel().map(MapMode.READ_WRITE, 0, 0);
        wrotePosition = new AtomicLong(randomAccessFile.length());
    }

    public void write(byte[] data) throws IOException {
        this.mappedByteBuffer =
                randomAccessFile.getChannel().map(MapMode.READ_WRITE, wrotePosition.get(), data.length);
        this.mappedByteBuffer.put(data);
        this.mappedByteBuffer.force();
        wrotePosition.addAndGet(data.length);
    }

    public byte[] read(long offset) throws IOException {
        ByteBuffer buffer = bufferThreadLocal.get();
        buffer.clear();
        this.randomAccessFile.getChannel().read(buffer, offset);
        return buffer.array();
    }
}
