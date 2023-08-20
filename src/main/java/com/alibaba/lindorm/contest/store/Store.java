package com.alibaba.lindorm.contest.store;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.utils.serializa.Serialization;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Schema;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class Store {

    Map<String, FileChannel> tableChannel;

    private void init() {
        this.tableChannel = new HashMap<>();
    }

    public Store() {
        init();
    }

    public synchronized void insert(String tableName, Collection<Row> rows) {
        FileChannel channel = getFileChannel(tableName);
        rows.stream().forEach(row -> {
            try {
                byte[] rowBytes = Serialization.serialize(row);
                ByteBuffer buffer = ByteBuffer.wrap(rowBytes);
                channel.write(buffer, channel.size());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void createTable(String tableName, Schema schema) {
        try {
            File tableFile = new File(tableName);
            if (!tableFile.exists()) {
                if (!tableFile.createNewFile()) {
                    throw new RuntimeException("创建文件失败");
                }
                RandomAccessFile randomAccessFile = new RandomAccessFile(tableFile, "rw");
                FileChannel channel = randomAccessFile.getChannel();
                tableChannel.put(tableName, channel);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized ArrayList<Row> queryLatestData(String tableName, Collection<Vin> vins, Set<String> requestedColumns) {
        ArrayList<Row> result = new ArrayList<>();
        FileChannel channel = getFileChannel(tableName);
        try {
            for (Vin vin : vins) {
                channel.position(0L);
                Row latestRow = null;
                while (true) {
                    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
                    if (channel.read(buffer) == -1 || channel.position() == channel.size()) {
                        break;
                    }
                    buffer.flip();
                    int totalSize = buffer.getInt();
                    ByteBuffer rowBuffer = ByteBuffer.allocate(totalSize - Integer.BYTES);
                    channel.read(rowBuffer);
                    rowBuffer.flip();
                    Row row = Serialization.deSerialize(rowBuffer);
                    if (vin.equals(row.getVin())) {
                        if (latestRow == null || latestRow.getTimestamp() <= row.getTimestamp()) {
                            latestRow = row;
                        }
                    }
                }
                if (latestRow != null) {
                    result.add(latestRow);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        filterByColumn(result, requestedColumns);
        return result;
    }

    public synchronized ArrayList<Row> queryTimeRangeData(String tableName, Vin vin, long timeLowerBound, long timeUpperBound, Set<String> requestedColumns) {
        Set<Row> result = new HashSet<>();
        FileChannel channel = getFileChannel(tableName);
        try {
            channel.position(0L);
            while (true) {
                ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
                if (channel.read(buffer) == -1 || channel.position() == channel.size()) {
                    break;
                }
                buffer.flip();
                int totalSize = buffer.getInt();
                ByteBuffer rowBuffer = ByteBuffer.allocate(totalSize - Integer.BYTES);
                channel.read(rowBuffer);
                rowBuffer.flip();
                Row row = Serialization.deSerialize(rowBuffer);
                if (vin.equals(row.getVin()) && row.getTimestamp() >= timeLowerBound && row.getTimestamp() < timeUpperBound) {
                    result.add(row);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        filterByColumn(result, requestedColumns);
        return new ArrayList<>(result);
    }

    private FileChannel getFileChannel(String tableName) {
        return tableChannel.computeIfAbsent(tableName, fileChannel -> {
            try {
                RandomAccessFile randomAccessFile = new RandomAccessFile(new File(tableName), "rw");
                return randomAccessFile.getChannel();
            } catch (IOException e) {
                throw new RuntimeException("创建或打开文件失败", e);
            }
        });
    }

    public void shutdown() {
        this.tableChannel.forEach((tableName, fileChannel) -> {
            try {
                fileChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        this.tableChannel.clear();
    }

    private void filterByColumn(Collection<Row> rows, Set<String> columns) {
        rows.forEach(row -> {
            Map<String, ColumnValue> rowColumns = row.getColumns();
            for (Iterator<Map.Entry<String, ColumnValue>> iterator = rowColumns.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<String, ColumnValue> val = iterator.next();
                if (!columns.contains(val.getKey())) {
                    iterator.remove();
                }
            }
        });
    }
}
