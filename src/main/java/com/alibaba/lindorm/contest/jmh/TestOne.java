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
import com.alibaba.lindorm.contest.structs.LatestQueryRequest;
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
        File dataDir = new File("temp");
        TSDBEngineImpl engine = new TSDBEngineImpl(dataDir);
        engine.connect();
//        String str = "12345678912345678";
//        ArrayList<Vin> vinList = new ArrayList<>();
//        vinList.add(new Vin(str.getBytes(StandardCharsets.UTF_8)));
//        Set<String> requestedColumns = new HashSet<>(Arrays.asList("col1", "col2", "col3"));
//        ArrayList<Row> resultSet = engine.executeLatestQuery(new LatestQueryRequest("test", vinList, requestedColumns));
//        System.out.println("resultSet: " + resultSet);
    }

}
