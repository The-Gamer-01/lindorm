package com.alibaba.lindorm.contest.util.cache;

import com.alibaba.lindorm.contest.storage.FileStorage;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.util.ColumnTs;
import com.alibaba.lindorm.contest.util.file.ByteUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class DoubleBufferCache implements Cache {

    private AtomicBoolean isSyncRunning;

    private FileStorage fileStorage;

    /**
     * key -> column offset
     */
    private volatile Map<String, List<ColumnTs>> currentBuffer;

    private volatile Map<String, List<ColumnTs>> syncBuffer;

    public DoubleBufferCache(String path) throws IOException {
        this.fileStorage.init(path);
        this.isSyncRunning = new AtomicBoolean(false);
        this.currentBuffer = new HashMap<>();
        this.syncBuffer = new HashMap<>();
    }

    public void put(String key, List<Row> rows) {
        synchronized (this) {
            if (isSyncRunning.get()) {
                while (isSyncRunning.get()) {
                    try {
                        wait(1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            swapReadyToSync();
            isSyncRunning.set(true);
        }
        synchronized (this) {
            isSyncRunning.set(false);
            notifyAll();
        }
    }

    public List<ColumnTs> getColumnTs(String key) {
        return currentBuffer.get(key);
    }

    public ColumnTs getLastColumnTs(String key) {
        List<ColumnTs> columnTs = getColumnTs(key);
        return columnTs.stream()
                .max(Comparator.comparingLong(ColumnTs::getTimestamp))
                .orElse(null);
    }

    private void swapReadyToSync() {
        Map<String, List<ColumnTs>> tmp = currentBuffer;
        currentBuffer = syncBuffer;
        syncBuffer = tmp;
    }

    public void flush() throws IOException {
        swapReadyToSync();
        for (Map.Entry<String, List<ColumnTs>> entry : syncBuffer.entrySet()) {
            String key = entry.getKey();
            List<ColumnTs> value = entry.getValue();
            value = value.stream()
                    .distinct()
                    .sorted()
                    .collect(Collectors.toList());
            fileStorage.write(key.getBytes(StandardCharsets.UTF_8), columnTsListToByte(value));
        }
    }

    public byte[] columnTsListToByte(List<ColumnTs> columnTsList) {
        List<byte[]> bytes = new ArrayList<>();
        int size = 0;
        for (ColumnTs columnTs : columnTsList) {
            long timestamp = columnTs.getTimestamp();
            byte[] columnValue = columnTs.getColumnValue();

            size += 8;
            bytes.add(ByteUtil.longToByte(timestamp));
            size += columnValue.length;
            bytes.add(columnValue);
        }
        byte[] result = new byte[size];
        int index = 0;
        for (byte[] aByte : bytes) {
            for (byte b : aByte) {
                result[index++] = b;
            }
        }
        return result;
    }
}
