package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.index.MemoryBufferIndex;
import com.alibaba.lindorm.contest.storage.FileStorage;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.ColumnValue.ColumnType;
import com.alibaba.lindorm.contest.structs.WriteRequest;
import com.alibaba.lindorm.contest.util.ColumnTs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.lindorm.contest.common.HighBitPartitioner.getPartition;
import static com.alibaba.lindorm.contest.common.NumberUtils.doubleToBytes;
import static com.alibaba.lindorm.contest.common.NumberUtils.intToBytes;
import static com.alibaba.lindorm.contest.common.TypeUtils.columnTypeOf;
import static java.util.Comparator.comparingLong;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-04
 */
public class InfluxBuffer {

    private static final int BUFFER_SIZE = 300; // 每块内存的大小

    private static final int BUFFER_THRESHOLD = 200;

    private static final int PREFIX_BYTE = 24;

    private ByteBuffer bufferA;

    private ByteBuffer bufferB;

    private ByteBuffer buffer;

    private Map<String, List<ColumnTs>> cache = new ConcurrentHashMap<>();

    private MemoryBufferIndex index;

    private volatile boolean writeToBuffer = true;

    private Task task = new Task();

    private Map<String, ColumnType> columnTypeMap = new ConcurrentHashMap<>();

    private FileStorage storage = new FileStorage();

    public InfluxBuffer(String path) throws IOException {
        bufferA = ByteBuffer.allocate(BUFFER_SIZE);
        bufferB = ByteBuffer.allocate(BUFFER_SIZE);
        index = new MemoryBufferIndex();
        storage.init(path);
//        task.execute(this::flush);
    }

    public void put(WriteRequest writeRequest) {
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

    public void flush() {
        buffer = writeToBuffer ? bufferA : bufferB;
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
            try {
                long offset = buffer.position() + storage.getMappedByteBufferWrappers()[getPartition(
                        key.getBytes())].getFilePosition();
                buffer.putLong(offset);
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
            });
            tempByteBuff.flip();
            // 删除key
            columnTypeMap.remove(key);
            buffer.put(tempByteBuff);
            System.out.println("buffer.position1:" + buffer.position());
            if (buffer.position() >= BUFFER_THRESHOLD) {
                System.out.println("buffer.position2:" + buffer.position());
                System.out.println("buffer: "+ buffer);
                writeToBuffer = !writeToBuffer;
                buffer.flip();
                try {
                    storage.write(key.getBytes(), buffer.array());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                buffer.clear();
                buffer = writeToBuffer ? bufferA : bufferB;
            }
        });
    }

    public byte[] read() {
        return null;
    }

    private byte[] columnValueToBytes(ColumnValue columnValue) {
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

    public ColumnType getType(String key) {
        return columnTypeMap.get(key);
    }

    public ColumnTs getLastColumnTs(String key) {
        List<ColumnTs> columnTsList =  cache.get(key);
        if (columnTsList == null) {
            System.err.println("cache key not found");
        }
        return columnTsList.stream()
                .max(comparingLong(ColumnTs::getTimestamp))
                .orElse(null);
    }
}
