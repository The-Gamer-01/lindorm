package com.alibaba.lindorm.contest.jmh;

import static com.alibaba.lindorm.contest.common.HighBitPartitioner.getPartition;
import static com.alibaba.lindorm.contest.common.NumberUtils.bytesToLong;
import static com.alibaba.lindorm.contest.common.NumberUtils.longTobytes;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

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
//        test1();
//        Options opt = new OptionsBuilder()
//                .include(TestTwo.class.getSimpleName())
//                .build();
//        new Runner(opt).run();
//        System.out.println("value1".getBytes().length);

        test();
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

    public static void test() throws Exception {
        FileStorage storage = new FileStorage();
        storage.init("temp");
        long offset = 128;
        byte[] data = longTobytes(offset);
        byte[] buffer = "data".getBytes();
        ByteBuffer buffer2 = ByteBuffer.allocate(8 + "data".getBytes().length);
        buffer2.putLong(1923);
        buffer2.put("data".getBytes());
        buffer2.flip();
        storage.write("key1".getBytes(), buffer2.array());
        byte[] res = storage.read("key1".getBytes(), 0);
        byte[] resNum = new byte[8];
        for (int i = 0; i < 8; i++) {
            resNum[i] = res[i];
        }
        System.out.println(bytesToLong(resNum));
    }

    private static byte[] mergeArrays(byte[] array1, byte[] array2) {
        byte[] mergedArray = new byte[array1.length + array2.length];
        System.arraycopy(array1, 0, mergedArray, 0, array1.length);
        System.arraycopy(array2, 0, mergedArray, array1.length, array2.length);
        return mergedArray;
    }
}
