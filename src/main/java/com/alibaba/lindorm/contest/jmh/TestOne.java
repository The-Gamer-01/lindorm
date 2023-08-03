package com.alibaba.lindorm.contest.jmh;

import static com.alibaba.lindorm.contest.Serialization.deserialize;
import static com.alibaba.lindorm.contest.Serialization.serialize;
import static com.alibaba.lindorm.contest.TSDBEngineImpl.getRealWriteReq;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;


import com.alibaba.lindorm.contest.TSDBEngineImpl;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.structs.WriteRequest;
import com.alibaba.lindorm.contest.util.RealWriteReq;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-07-31
 */
public class TestOne {

    public static void main(String[] args) throws Exception {
//        TSDBEngineImpl engine = new TSDBEngineImpl(new File("temp"));
//        engine.connect();
////        engine.createTable("azh", null);
//        for (int i = 0; i < 10; i++) {
//            engine.upsert(request());
//        }
//        System.out.println(engine.getCache().getQueue().size());
//        engine.shutdown();



//        Fury fury = Fury.builder()
//                .withLanguage(Language.JAVA)
//                .build();
//
//        fury.serialize(request());


//        byte[] stringBytes = "data".getBytes(StandardCharsets.UTF_8);
//        ByteBuffer buffer = ByteBuffer.allocate(stringBytes.length);
//        // 向缓冲区写入数据
//        buffer.clear();
//        buffer.put(stringBytes);
//        buffer.flip();
//
//        //  buffer
//        AsynchronousFileChannel fileChannel =
//                AsynchronousFileChannel.open(Path.of("data"), StandardOpenOption.WRITE, StandardOpenOption.READ,
//                        StandardOpenOption.CREATE);
//
//        fileChannel.write(buffer, fileChannel.size());

//        RealWriteReq req = new RealWriteReq();
//
//        byte[] str = "azh ttt".getBytes();
////        req.setCrc(11);
////        req.setValueOffset(123);
////        System.out.println(Arrays.toString(serialize(req)));
////        System.out.println(deserialize(serialize(req)));
//        AsynchronousFileChannel fileChannel =
//                AsynchronousFileChannel.open(Path.of("zzh"), StandardOpenOption.WRITE, StandardOpenOption.READ,
//                        StandardOpenOption.CREATE);
//
//        fileChannel.write(ByteBuffer.wrap(str), fileChannel.size());

//        testWriteFile3();
//        testWriteFile();
//        testWriteFile2();
        test4();
    }


    private static void test4() {
        long value = 123456789L;

        // 将long转换为byte数组
        byte[] byteArray = new byte[8];
        for (int i = 0; i < 8; i++) {
            byteArray[i] = (byte) (value >> (56 - (i * 8)));
        }

        System.out.println("long值 " + value + " 转换为byte数组为:");
        for (byte b : byteArray) {
            System.out.print(b + " ");
        }
        System.out.println(Arrays.toString(byteArray));
    }

    public static void testWriteFile3() {
        Path filePath = Path.of("zzh3");

        int decimal = 10;
        String binary = Integer.toBinaryString(decimal);

        // 计算二进制字符串的长度
        int numBits = binary.length();

        try {
            RandomAccessFile fileChannel = new RandomAccessFile("output.txt", "rw");

            fileChannel.write(binary.getBytes());

//            fileChannel.close();
            byte[] data = new byte[numBits];

            fileChannel.read(data);
            System.out.println(Arrays.toString(data));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    static int fileOffset = 0;

    public static void testWriteFile2() throws Exception {
        ByteBuffer flushBuf = ByteBuffer.allocate(1000);

        AsynchronousFileChannel fileChannel =
                AsynchronousFileChannel.open(Path.of("zzh2"), StandardOpenOption.WRITE, StandardOpenOption.READ,
                        StandardOpenOption.CREATE);
//        flushBuf.put("a2322322323222sdfsdfdsfsdahz".getBytes());
//        flushBuf.flip();


        long decimal = System.currentTimeMillis();
        String binary = Long.toBinaryString(decimal);

        // 计算二进制字符串的长度
        int numBits = binary.length();
        System.out.println("len: " + numBits);
        ByteBuffer buffer = ByteBuffer.allocate(numBits);
        buffer.put(binary.getBytes());

        buffer.flip();

        fileChannel.write(buffer, fileChannel.size());
        fileChannel.force(true);

        ByteBuffer buffer2 = ByteBuffer.allocate(Long.BYTES);
        fileChannel.read(buffer2, 0);
        System.out.println("buffer2: " + buffer2);
        buffer2.flip();
        byte[] data = new byte[buffer2.remaining()];
        buffer2.get(data);
        String content = new String(data);
        System.out.println(content);
    }

    public static void testWriteFile() throws Exception {
        List<RealWriteReq> request = getRealWriteReq(request());

        //        req.setCrc(11);
        //        req.setValueOffset(123);
        //        System.out.println(Arrays.toString(serialize(req)));
        //        System.out.println(deserialize(serialize(req)));
        AsynchronousFileChannel fileChannel =
                AsynchronousFileChannel.open(Path.of("zzh"), StandardOpenOption.WRITE, StandardOpenOption.READ,
                        StandardOpenOption.CREATE);
        ByteBuffer flushBuf = ByteBuffer.allocate(serialize(request).length);

        request.forEach(it -> {
            // todo ts需要后续做索引，查最新ts
            long ts = it.getTs();

            // 构造vin索引
            String vin = Arrays.toString(it.getVin());
            int vinLen = vin.getBytes(StandardCharsets.UTF_8).length;
            long fileOffset = 0;
            try {
                fileOffset = fileChannel.size();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            long vinOffset = vinLen + fileOffset;

            byte[] key = it.getKey();
            byte[] value = it.getValue();

            int keyLen = key.length;
            long keyOffset = keyLen + vinOffset;
            int valueLen = value.length;
            long valueOffset = valueLen + keyOffset;

            ByteBuffer buffer = ByteBuffer.allocate(32 + vinLen + keyLen + it.getValueType().length + valueLen + 1000);
            buffer.putInt(it.getCrc());
            buffer.putLong(fileOffset);
            buffer.putLong(it.getTs());
            buffer.putInt(vinLen);
            buffer.putInt(keyLen);
            buffer.putInt(valueLen);
            buffer.put(vin.getBytes(StandardCharsets.UTF_8));
            buffer.put(key);
            buffer.put(it.getValueType());
            buffer.put(value);
            //                    System.out.println("decoder buffer: "+decoder(buffer));
            buffer.flip();
            flushBuf.put(buffer);
            fileOffset = valueOffset;
        });
        flushBuf.flip();

        fileChannel.write(flushBuf, fileChannel.size());
    }

    private static WriteRequest request() {
        Map<String, ColumnValue> columns = new HashMap<>();
        ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.put((byte) 70);
        buffer.put((byte) 71);
        buffer.put((byte) 72);
        columns.put("col1", new ColumnValue.IntegerColumn(123));
        columns.put("col2", new ColumnValue.DoubleFloatColumn(1.23));
        columns.put("col3", new ColumnValue.StringColumn(buffer));
        String str = "12345678912345678";
        ArrayList<Row> rowList = new ArrayList<>();
        rowList.add(new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), System.currentTimeMillis(), columns));
        return new WriteRequest("test", rowList);
    }

    private static String decoder(ByteBuffer buffer) {
        try {
            Charset charset = StandardCharsets.UTF_8;
            CharsetDecoder decoder = charset.newDecoder();
            buffer.flip();
            CharBuffer buffer1 = decoder.decode(buffer);
            return buffer1.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "xx";
    }
}
