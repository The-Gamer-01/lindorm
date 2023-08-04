package com.alibaba.lindorm.contest.jmh;

import static com.alibaba.lindorm.contest.example.EvaluationSample.showResult;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.alibaba.lindorm.contest.TSDBEngineImpl;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.LatestQueryRequest;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.structs.WriteRequest;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-05
 */
public class InfluexBufferTest {

    public static void main(String[] args) throws IOException {
        File dataDir = new File("temp");
        TSDBEngineImpl engine = new TSDBEngineImpl(dataDir);
        engine.connect();

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
        rowList.add(new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), 1, columns));
        for (int i = 0; i < 1; i++) {
//            try {
//                Thread.sleep(1);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
            engine.upsert((new WriteRequest("test", rowList)));
//            ArrayList<Vin> vinList = new ArrayList<>();
//            vinList.add(new Vin(str.getBytes(StandardCharsets.UTF_8)));
//            Set<String> requestedColumns = new HashSet<>(Arrays.asList("col1", "col2", "col3"));
//            ArrayList<Row> resultSet = engine.executeLatestQuery(new LatestQueryRequest("test", vinList, requestedColumns));
//            System.out.println("resultSet: " + resultSet);
        }
//        engine.upsert((new WriteRequest("test", rowList)));
//        ArrayList<Vin> vinList = new ArrayList<>();
//        vinList.add(new Vin(str.getBytes(StandardCharsets.UTF_8)));
//        Set<String> requestedColumns = new HashSet<>(Arrays.asList("col1", "col2", "col3"));
//        ArrayList<Row> resultSet = engine.executeLatestQuery(new LatestQueryRequest("test", vinList, requestedColumns));
//        showResult(resultSet);
        // partition: 196
//        engine.upsert((new WriteRequest("test", rowList)));
        engine.shutdown();
    }
}
