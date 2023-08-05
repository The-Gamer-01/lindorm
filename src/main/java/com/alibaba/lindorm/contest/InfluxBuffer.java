package com.alibaba.lindorm.contest;

import static com.alibaba.lindorm.contest.common.FileUtils.traverseFolder;
import static com.alibaba.lindorm.contest.common.HighBitPartitioner.getPartition;
import static com.alibaba.lindorm.contest.common.NumberUtils.bytesToInt;
import static com.alibaba.lindorm.contest.common.NumberUtils.doubleToBytes;
import static com.alibaba.lindorm.contest.common.NumberUtils.intToBytes;
import static com.alibaba.lindorm.contest.common.TypeUtils.columnTypeOf;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import com.alibaba.lindorm.contest.common.TwoTuple;
import com.alibaba.lindorm.contest.index.IndexBlockEntry;
import com.alibaba.lindorm.contest.index.InfluxIndex;
import com.alibaba.lindorm.contest.index.MemoryBufferIndex;
import com.alibaba.lindorm.contest.storage.FileStorage;
import com.alibaba.lindorm.contest.storage.MappedByteBufferWrapper;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.ColumnValue.ColumnType;
import com.alibaba.lindorm.contest.structs.WriteRequest;
import com.alibaba.lindorm.contest.util.ColumnTs;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-04
 */
public class InfluxBuffer {

    private static final int BUFFER_SIZE = 32 * 1024; // 每块内存的大小

    private static final int BUFFER_THRESHOLD = 30 * 1024;

    private static final int PREFIX_BYTE = 24;

    private ByteBuffer bufferA;

    private ByteBuffer bufferB;

    private ByteBuffer buffer;

    private Map<String, List<ColumnTs>> cache = new ConcurrentHashMap<>();

    private volatile boolean writeToBuffer = true;

    private Task task = new Task();

    private Map<String, ColumnType> columnTypeMap = new ConcurrentHashMap<>();

    private FileStorage storage = new FileStorage();

    private InfluxIndex index;

    public InfluxBuffer(String path) throws IOException {
        bufferA = ByteBuffer.allocate(BUFFER_SIZE);
        bufferB = ByteBuffer.allocate(BUFFER_SIZE);
        buffer = writeToBuffer ? bufferA : bufferB;
        index = new InfluxIndex();
        storage.init(path);
        //        task.execute(this::flush);
    }

    public void load() {
        Arrays.stream(storage.getMappedByteBufferWrappers()).forEach(mappedBuffer -> {
            try {
                TwoTuple<MappedByteBuffer, Long> tuple = mappedBuffer.load();
                MappedByteBuffer byteBuffer = tuple.getFirst();
                while (byteBuffer.position() != byteBuffer.limit()) {
                    // 读取keyLen
                    int keyLen = byteBuffer.getInt();
                    System.out.println("keyLen: " + keyLen);
                    System.out.println(byteBuffer.position());
                    // 读取key
                    byteBuffer.position(byteBuffer.position() + 4);
                    byte[] data = new byte[keyLen];
                    byteBuffer.get(data, 0, keyLen);
                    // 读取minTs
                    byteBuffer.position(byteBuffer.position() + keyLen);
                    long minTs = byteBuffer.getLong();
                    // 读取maxTs
                    byteBuffer.position(byteBuffer.position() + 8);
                    long maxTs = byteBuffer.getLong();
                    // 读取 value的offset
                    byteBuffer.position(byteBuffer.position() + 8);
                    long offset = byteBuffer.getLong();
                    byteBuffer.position(byteBuffer.position() + 8);
                    int size = byteBuffer.getInt();
                    index.getBufferIndex().put(new String(data), new IndexBlockEntry(minTs, maxTs, offset, size));
                    // 跳跃datablock字节
                    byteBuffer.position(byteBuffer.position() + 4);
                    int valLen = byteBuffer.getInt();
                    int tsLen = size * 8;
                    byteBuffer.position(byteBuffer.position() + 4 + tsLen + valLen);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
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
                long offset = storage.getMappedByteBufferWrappers()[getPartition(key.getBytes())].getFilePosition();
                buffer.putLong(offset);
                buffer.putInt(columnTsList.size());
                IndexBlockEntry entry = new IndexBlockEntry(minTs, maxTs, offset, columnTsList.size());
                index.getBufferIndex().put(key, entry);
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
//            System.out.println("buffer.position1:" + buffer.position());
            if (buffer.position() >= BUFFER_THRESHOLD) {
//                System.out.println("buffer.position2:" + buffer.position());
//                System.out.println("buffer: " + buffer);
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

    public ColumnTs getLastColumnTs(String key) throws IOException {
        List<ColumnTs> columnTsList = cache.get(key);
        if (columnTsList != null) {
            return columnTsList.stream()
                    .max(comparingLong(ColumnTs::getTimestamp))
                    .orElse(null);
        }
        System.err.println("cache key not found");
        // read disk
        IndexBlockEntry entry = index.getBufferIndex().get(key);
        if (entry == null) {
            throw new RuntimeException("entry is null");
        }
        long offset = entry.getOffset();
        int size = entry.getSize();
        byte[] data = storage.read(key.getBytes(), offset, 4);
        int valueLen = bytesToInt(data);
        int tsSize = size * 8;
        byte[] timestampBytes = storage.read(key.getBytes(), offset + 4, tsSize);
        ByteBuffer timeBuffer = ByteBuffer.wrap(timestampBytes);
        long ts = IntStream.range(0, size)
                .mapToObj(i -> timeBuffer.getLong())
                .collect(toList()).get(size - 1);
        byte[] valueBytes = storage.read(key.getBytes(), offset + tsSize + 4, valueLen);
        byte[] value = Arrays.stream(new String(valueBytes).split("#_#"))
                .reduce((first, second) -> second)
                .orElse("").getBytes();
        return new ColumnTs(ts, value);
    }

    public InfluxIndex getIndex() {
        return this.index;
    }
}
