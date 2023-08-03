package com.alibaba.lindorm.contest.jmh;

import static com.alibaba.lindorm.contest.common.HighBitPartitioner.getPartition;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.alibaba.lindorm.contest.storage.FileStorage;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-02
 */
@State(Scope.Thread)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@Fork(1)
public class TestTwo {

    public static void main(String[] args) throws Exception {
//        String filePath = "mmap_file";
//        long currentSize = "data".getBytes().length; // 当前文件大小，需获取或记录
//
//        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw");
//                FileChannel channel = file.getChannel()) {
//
//            // 计算要追加数据的位置和大小
//            long appendedSize = "data".getBytes().length/* 计算要追加的数据大小 */;
//            long position = currentSize; // 追加的起始位置
//
//            // 映射文件部分内容到内存
//            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, position, appendedSize);
//
//            // 在映射的内存中写入要追加的数据
//            /* 将要追加的数据写入 buffer */
//            buffer.put("data".getBytes());
//
//            // 刷新缓冲区
//            buffer.force();
//
//            // 更新当前文件大小
//            currentSize += appendedSize;
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        test1();
        Options opt = new OptionsBuilder()
                .include(TestTwo.class.getSimpleName())
                .build();
        new Runner(opt).run();
//        System.out.println("value1".getBytes().length);
    }

    FileStorage storage = new FileStorage();
    @Setup
    public void setup() throws IOException {
        storage.init("temp");
    }


    @Benchmark
    public void testWrite() throws IOException {
        storage.write("key1".getBytes(), "value1".getBytes());
    }

    @Benchmark
    public void testRead() throws IOException {
        byte[] data = storage.read("key1".getBytes(), 0);
    }
}
