package com.alibaba.lindorm.contest.jmh;

import static com.alibaba.lindorm.contest.common.HighBitPartitioner.getPartition;
import static com.alibaba.lindorm.contest.common.NumberUtils.bytesToLong;
import static com.alibaba.lindorm.contest.common.NumberUtils.longTobytes;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
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

import com.alibaba.lindorm.contest.index.IndexBlockEntry;
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

//        test3();
        test3();
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
        storage.init("temp2");
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

    public static void test2() throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(new File("test"), "rw");
        String str = "5678912345678_col3";
        ByteBuffer buffer =
                ByteBuffer.allocate(str.getBytes().length + 4);
        buffer.putInt(str.getBytes().length);
        buffer.put(str.getBytes());
        buffer.flip();
        randomAccessFile.write(buffer.array());

        long fileSize = randomAccessFile.getChannel().size();
        MappedByteBuffer byteBuffer = randomAccessFile.getChannel().map(MapMode.READ_ONLY, 0, fileSize);
        int keyLen = byteBuffer.getInt();

        System.out.println("keyLen: " + keyLen);

        // 读取key
        byte[] data = new byte[keyLen];
        byteBuffer.get(data, 0, keyLen);
        System.out.println("key: " + new String(data));
    }

    public static void test3() throws IOException {
        String path = "temp/196";
        RandomAccessFile randomAccessFile = new RandomAccessFile(path, "rw");

        long fileSize = randomAccessFile.getChannel().size();
        MappedByteBuffer byteBuffer = randomAccessFile.getChannel().map(MapMode.READ_ONLY, 0, fileSize);
        byteBuffer.position(0);
        while (byteBuffer.position() != byteBuffer.limit()) {
            // 读取keyLen
            int keyLen = byteBuffer.getInt();
            System.out.println("keyLen: " + keyLen);
            System.out.println(byteBuffer.position());

            // 读取key
            byte[] data = new byte[keyLen];
            byteBuffer.get(data, 0, keyLen);
            System.out.println("key: " + new String(data));
            // 读取minTs
            long minTs = byteBuffer.getLong();
            // 读取maxTs
            long maxTs = byteBuffer.getLong();
            // 读取 value的offset
            long offset = byteBuffer.getLong();
            int size = byteBuffer.getInt();
            // 跳跃datablock字节

            int valLen = byteBuffer.getInt();
            int tsLen = size * 8;
            byteBuffer.position(byteBuffer.position() + valLen + tsLen);
        }
    }
}
