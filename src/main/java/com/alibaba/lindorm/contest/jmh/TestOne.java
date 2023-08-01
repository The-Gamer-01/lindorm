package com.alibaba.lindorm.contest.jmh;

import static com.alibaba.lindorm.contest.Serialization.deserialize;
import static com.alibaba.lindorm.contest.Serialization.serialize;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.SerializationUtils;

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
        TSDBEngineImpl engine = new TSDBEngineImpl(new File("temp"));
        engine.connect();
//        engine.createTable("azh", null);
        for (int i = 0; i < 10; i++) {
            engine.upsert(request());
        }
        System.out.println(engine.getCache().getQueue().size());
        engine.shutdown();



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
//        req.setCrc(11);
//        req.setValueOffset(123);
//        System.out.println(Arrays.toString(serialize(req)));
//        System.out.println(deserialize(serialize(req)));
//        AsynchronousFileChannel fileChannel =
//                AsynchronousFileChannel.open(Path.of("zzh"), StandardOpenOption.WRITE, StandardOpenOption.READ,
//                        StandardOpenOption.CREATE);
//
//        fileChannel.write(ByteBuffer.wrap(str), fileChannel.size());
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
}
