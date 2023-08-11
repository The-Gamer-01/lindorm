package com.alibaba.lindorm.contest.v2.buffer;

import com.alibaba.lindorm.contest.index.DefaultIndex;
import com.alibaba.lindorm.contest.index.Index;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.util.*;

public class DefaultCache implements Cache {

    private final Index index = DefaultIndex.getInstance();

    private static Cache cache;

    private volatile List<Row> currentRows;

    private volatile List<Row> syncRows;

    public synchronized static Cache getInstance() {
        if (cache == null) {
            cache = new DefaultCache();
        }
        return cache;
    }

    private DefaultCache() {
        this.currentRows = new ArrayList<>();
        this.syncRows = new ArrayList<>();
    }

    public void write(String tableName, Row row) {
        currentRows.add(row);
    }

    public void flush() {
//        this.syncKeyPairMap = keyPairMap;
//        this.keyPairMap = new HashMap<>();
//        for (Map.Entry<String, List<Pair<Long, ColumnValue>>> entry : keyPairMap.entrySet()) {
//            String key = entry.getKey();
//            int offset = index.getOffset(key);
//            List<Pair<Long, ColumnValue>> value = entry.getValue();
//
//            int length = value.size();
//            List<Long> timestamps = new ArrayList<>();
//            List<ColumnValue> columnValues = new ArrayList<>();
//            for (Pair<Long, ColumnValue> pair : value) {
//                timestamps.add(pair.first());
//                columnValues.add(pair.second());
//            }
//
//            byte[] lengthBytes = ByteUtil.intToByte(length);
//            byte[] timestampsBytes = ByteUtil.longArrayToByte(timestamps.toArray(new Long[0]));
//
//            int bufferSize = lengthBytes.length + timestampsBytes.length;
//            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
//            buffer.put(lengthBytes);
//            buffer.put(timestampsBytes);
//
//            Store store = DefaultStore.getInstance();
//            store.write(offset, buffer);
//        }
    }

    @Override
    public ArrayList<Row> getLatestRowsByVins(String tableName, Collection<Vin> vins, Set<String> columns) {
        Map<Row, Long> row2TimeMap = new HashMap<>();
        for (Row row : currentRows) {
            Long aLong = row2TimeMap.get(row);
            if (aLong == null) {
                row2TimeMap.put(row, row.getTimestamp());
            } else {
                if (aLong < row.getTimestamp()) {
                    row2TimeMap.put(row, row.getTimestamp());
                }
            }
        }
        ArrayList<Row> result = new ArrayList<>();
        for (Map.Entry<Row, Long> entry : row2TimeMap.entrySet()) {
            Row row = entry.getKey();
            Map<String, ColumnValue> stringColumnValueMap = new HashMap<>();
            Map<String, ColumnValue> rowColumns = row.getColumns();
            for (String column : columns) {
                if (rowColumns.containsKey(column)) {
                    stringColumnValueMap.put(column, rowColumns.get(column));
                }
            }
            Row newRow = new Row(row.getVin(), row.getTimestamp(), stringColumnValueMap);
            result.add(newRow);
        }
        return result;
    }
}
