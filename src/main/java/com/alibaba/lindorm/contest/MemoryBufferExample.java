package com.alibaba.lindorm.contest;

import static com.alibaba.lindorm.contest.common.NumberUtils.doubleToBytes;
import static com.alibaba.lindorm.contest.common.NumberUtils.intToBytes;
import static com.alibaba.lindorm.contest.common.TypeUtils.columnTypeOf;

import java.nio.ByteBuffer;

import com.alibaba.lindorm.contest.index.MemoryBufferIndex;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.WriteRequest;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-04
 */
public class MemoryBufferExample {

    private static final int BUFFER_SIZE = 32 * 1024 * 1024; // 每块内存的大小

    private static final int PREFIX_BYTE = 24;

    private ByteBuffer bufferA;

    private ByteBuffer bufferACount;

    private ByteBuffer bufferB;

    private ByteBuffer bufferBCount;

    private ByteBuffer buffer;

    private MemoryBufferIndex index;

    private static boolean writeToBuffer = true;

    public MemoryBufferExample() {
         bufferA = ByteBuffer.allocate(BUFFER_SIZE);
         bufferB = ByteBuffer.allocate(BUFFER_SIZE);
        index = new MemoryBufferIndex();
    }

    public void put(WriteRequest writeRequest) {
        buffer = writeToBuffer ? bufferA : bufferB;
        writeRequest.getRows().forEach(row -> {
            byte[] vin = row.getVin().getVin();
            long timestamp = row.getTimestamp();
            row.getColumns().forEach((columnName, columnValue) -> {
                byte[] columnNameBytes = columnName.getBytes();
                byte[] columnTypeBytes = columnValue.getColumnType().name().getBytes();
                byte[] columnValueBytes = columnValueToBytes(columnValue);
                int size = PREFIX_BYTE + vin.length + columnNameBytes.length + columnTypeBytes.length
                        + columnValueBytes.length;
                ByteBuffer tempBuffer = ByteBuffer.allocate(size);
                tempBuffer.putInt(vin.length);
                tempBuffer.putInt(columnNameBytes.length);
                tempBuffer.putInt(columnTypeOf(columnValue.getColumnType()));
                tempBuffer.putInt(columnValueBytes.length);
                tempBuffer.putLong(timestamp);
                tempBuffer.put(vin);
                tempBuffer.put(columnNameBytes);
                tempBuffer.put(columnValueBytes);
                tempBuffer.flip();
                buffer.put(tempBuffer);
                String key = vin + columnName;
//                index.getBufferIndex().put()
            });
        });
    }

    public void read() {

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
}
