package com.alibaba.lindorm.contest.jmh;

import static com.alibaba.lindorm.contest.common.HighBitPartitioner.getPartition;
import static com.alibaba.lindorm.contest.common.NumberUtils.doubleToBytes;
import static com.alibaba.lindorm.contest.common.NumberUtils.intToBytes;
import static com.alibaba.lindorm.contest.common.TypeUtils.columnTypeOf;
import static com.alibaba.lindorm.contest.example.EvaluationSample.showResult;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.lindorm.contest.TSDBEngineImpl;
import com.alibaba.lindorm.contest.index.IndexBlockEntry;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.ColumnValue.ColumnType;
import com.alibaba.lindorm.contest.structs.LatestQueryRequest;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.structs.WriteRequest;
import com.alibaba.lindorm.contest.util.ColumnTs;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-05
 */
public class InfluexBufferTest {

    private static Map<String, List<ColumnTs>> cache = new ConcurrentHashMap<>();

    private static Map<String, ColumnType> columnTypeMap = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException {
        init();

        write();
    }

    private static void write() throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(new File("test"), "rw");
        final ByteBuffer buffer = ByteBuffer.allocate(32 * 1024);
        cache.keySet().forEach(key -> {
            List<ColumnTs> columnTsList = cache.get(key);

            // 写索引[keyLen, key, type, count] meta & entry
            int keyLen = key.getBytes().length;
            buffer.putInt(keyLen);
            buffer.put(key.getBytes());
            buffer.putInt(columnTypeOf(columnTypeMap.get(key)));

            long minTs = Long.MAX_VALUE;
            long maxTs = Long.MIN_VALUE;

            int valueLen = 0;
            List<Long> timestamps = new ArrayList<>();

            for (ColumnTs columnTs : columnTsList) {
                long currentTs = columnTs.getTimestamp();
                valueLen += columnTs.getColumnValue().length;
                valueLen += "#_#".getBytes().length;
                timestamps.add(currentTs);

                if (currentTs < minTs) {
                    minTs = currentTs;
                }

                if (currentTs > maxTs) {
                    maxTs = currentTs;
                }
            }

            buffer.putLong(minTs);
            buffer.putLong(maxTs);
            buffer.putInt(columnTypeOf(columnTypeMap.get(key)));
            buffer.putInt(valueLen);
            try {
                long offset = randomAccessFile.getChannel().size();
                buffer.putLong(offset);
                buffer.putInt(columnTsList.size());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            // 写存储
            ByteBuffer tempByteBuff = ByteBuffer.allocate(valueLen);
            timestamps.forEach(ts -> {
                buffer.putLong(ts);
            });
            columnTsList.forEach(columnTs -> {
                tempByteBuff.put(columnTs.getColumnValue());
                tempByteBuff.put("#_#".getBytes());
            });
            tempByteBuff.flip();
            // 删除key
            columnTypeMap.remove(key);
            buffer.put(tempByteBuff);
            cache.remove(key);
//            if (buffer.position() >= 30 * 1024) {
            ByteBuffer flushBuf = ByteBuffer.allocate(buffer.position());
            buffer.flip();
            flushBuf.put(buffer);
            flushBuf.flip();
                try {
                    randomAccessFile.write(flushBuf.array());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                buffer.clear();
//            }
        });
    }

    private static void init() {
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
        WriteRequest writeRequest = new WriteRequest("test", rowList);

        writeRequest.getRows().forEach(row -> {
            String vin = new String(row.getVin().getVin());
            long timestamp = row.getTimestamp();
            row.getColumns().forEach((columnName, columnValue) -> {
                byte[] columnValueBytes = columnValueToBytes(columnValue);
                List<ColumnTs> columnTsList = cache.getOrDefault(String.join("_", vin, columnName), new ArrayList<>());
                columnTsList.add(new ColumnTs(timestamp, columnValueBytes));
                String key = String.join("_", vin, columnName);
                cache.put(key, columnTsList);
                columnTypeMap.put(key, columnValue.getColumnType());
            });
        });
    }

    private static void testWrite() {

    }



    private static void test() throws IOException {
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
        for (int j = 0; j < 100; j++) {
            for (int i = 0; i < 20; i++) {
                engine.upsert((new WriteRequest("test", rowList)));
            }
            engine.shutdown();
        }
    }

    private static byte[] columnValueToBytes(ColumnValue columnValue) {
        switch (columnValue.getColumnType()) {
            case COLUMN_TYPE_STRING:
                return columnValue.getStringValue().array();
            case COLUMN_TYPE_INTEGER:
                return intToBytes(columnValue.getIntegerValue());
            case COLUMN_TYPE_DOUBLE_FLOAT:
                return doubleToBytes(columnValue.getDoubleFloatValue());
        }
        return new byte[] {};
    }
}
