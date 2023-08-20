package com.alibaba.lindorm.contest.utils.serializa;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class Serialization {
    public static byte[] serialize(Row row) {
        int totalSize = 0;
        totalSize += Long.BYTES;
        totalSize += Integer.BYTES + row.getVin().getVin().length;

        for (Map.Entry<String, ColumnValue> entry : row.getColumns().entrySet()) {
            String key = entry.getKey();
            ColumnValue value = entry.getValue();
            totalSize += Integer.BYTES + key.getBytes(StandardCharsets.UTF_8).length; // key.getBytes().length and key.getBytes(StandardCharsets.UTF_8)
            totalSize += Integer.BYTES + Integer.BYTES + value.toBytes().length; // value.getType() and value.toBytes()
        }

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + totalSize);
        buffer.putInt(Integer.BYTES + totalSize);
        buffer.putLong(row.getTimestamp());
        buffer.putInt(row.getVin().getVin().length);
        buffer.put(row.getVin().getVin());
        for (Map.Entry<String, ColumnValue> entry : row.getColumns().entrySet()) {
            String key = entry.getKey();
            ColumnValue value = entry.getValue();
            buffer.putInt(key.getBytes().length);
            buffer.put(key.getBytes(StandardCharsets.UTF_8));

            buffer.putInt(value.getType());
            buffer.putInt(value.toBytes().length);
            buffer.put(value.toBytes());
        }
        return buffer.array();
    }

    public static Row deSerialize(ByteBuffer buffer) {
        long timestamp = buffer.getLong();
        int vinSize = buffer.getInt();
        byte[] vin = new byte[vinSize];
        buffer.get(vin);
        Map<String, ColumnValue> columnValueMap = new HashMap<>();
        while (buffer.hasRemaining()) {
            int len = buffer.getInt();
            byte[] columnBytes = new byte[len];
            buffer.get(columnBytes);
            String column = new String(columnBytes);

            int type = buffer.getInt();
            int valueLen = buffer.getInt();
            if (type == 0) {
                int intVal = buffer.getInt();
                columnValueMap.put(column, new ColumnValue.IntegerColumn(intVal));
            } else if (type == 1) {
                double doubleVal = buffer.getDouble();
                columnValueMap.put(column, new ColumnValue.DoubleFloatColumn(doubleVal));
            } else if (type == 2) {
                byte[] bytes = new byte[valueLen];
                buffer.get(bytes);
                ByteBuffer strVal = ByteBuffer.wrap(bytes);
                columnValueMap.put(column, new ColumnValue.StringColumn(strVal));
            }
        }
        return new Row(new Vin(vin), timestamp, columnValueMap);
    }
}
