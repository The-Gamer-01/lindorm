package com.alibaba.lindorm.contest.util;

import com.alibaba.lindorm.contest.pojo.data.DataBlock;
import com.alibaba.lindorm.contest.pojo.index.IndexBlock;
import com.alibaba.lindorm.contest.pojo.index.IndexBlockEntry;
import com.alibaba.lindorm.contest.pojo.index.IndexBlockMeta;
import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 序列化工具类
 */
public class SerializationUtil {

    public static ByteBuffer serializationDataBlock(DataBlock dataBlock) {
        Integer type = dataBlock.getType();
        Integer length = dataBlock.getLength();
        List<Long> timestamps = dataBlock.getTimestamps();
        List<ColumnValue> columnValues = dataBlock.getColumnValues();
        ByteBuffer buffer = ByteBuffer.allocate(getDataBlockByteSize(dataBlock));
        buffer.putInt(type);
        buffer.putInt(length);
        for (Long timestamp : timestamps) {
            buffer.putLong(timestamp);
        }
        for (ColumnValue columnValue : columnValues) {
            if (columnValue instanceof ColumnValue.StringColumn) {
                ByteBuffer stringValue = columnValue.getStringValue();
                buffer.putInt(stringValue.capacity());
                buffer.put(stringValue);
            } else if (columnValue instanceof ColumnValue.IntegerColumn) {
                int integerValue = columnValue.getIntegerValue();
                buffer.putInt(integerValue);
            } else if (columnValue instanceof ColumnValue.DoubleFloatColumn) {
                buffer.putDouble(columnValue.getDoubleFloatValue());
            }
        }
        return buffer;
    }

    public static ByteBuffer serializationIndexBlock(IndexBlock indexBlock) {
        IndexBlockMeta meta = indexBlock.getMeta();
        List<IndexBlockEntry> entries = indexBlock.getEntries();

        ByteBuffer buffer = ByteBuffer.allocate(getIndexBlockSize(indexBlock));
        buffer.putLong(meta.getKeyLength());
        buffer.put(meta.getKey().getBytes(StandardCharsets.UTF_8));
        buffer.putInt(meta.getType());
        buffer.putInt(meta.getCount());

        for (IndexBlockEntry entry : entries) {
            buffer.putLong(entry.getMinTime());
            buffer.putLong(entry.getMaxTime());
            buffer.putInt(entry.getOffset());
            buffer.putInt(entry.getSize());
        }
        return buffer;
    }

    public static ByteBuffer serializationFooter(int indexOffset) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
        buffer.putInt(indexOffset);
        return buffer;
    }

    private static int getDataBlockByteSize(DataBlock dataBlock) {
        int typeSize = Integer.SIZE / Byte.SIZE;
        int lengthSize = Integer.SIZE / Byte.SIZE;
        int timestampsSize = dataBlock.getTimestamps().size() * (Long.SIZE / Byte.SIZE);
        List<ColumnValue> columnValues = dataBlock.getColumnValues();
        int columnValueSize = 0;
        for (ColumnValue columnValue : columnValues) {
            if (columnValue instanceof ColumnValue.StringColumn) {
                columnValueSize += Long.SIZE / Byte.SIZE;
                columnValueSize += columnValue.getStringValue().capacity();
            } else if (columnValue instanceof ColumnValue.IntegerColumn) {
                columnValueSize += (Integer.SIZE / Byte.SIZE);
            } else if (columnValue instanceof ColumnValue.DoubleFloatColumn) {
                columnValueSize += Double.SIZE / Byte.SIZE;
            }
        }
        return typeSize + lengthSize + timestampsSize + columnValueSize;
    }

    private static int getIndexBlockSize(IndexBlock indexBlock) {
        IndexBlockMeta meta = indexBlock.getMeta();
        int keyLengthSize = Long.SIZE / Byte.SIZE;
        int keySize = meta.getKey().length();
        int typeSize = Integer.SIZE / Byte.SIZE;
        int countSize = Integer.SIZE / Byte.SIZE;
        int metaSize = keyLengthSize + keySize + typeSize + countSize;

        List<IndexBlockEntry> entries = indexBlock.getEntries();
        int entriesSize = entries.size();
        int minTimeSize = entriesSize * (Long.SIZE / Byte.SIZE);
        int maxTimeSize = entriesSize * (Long.SIZE / Byte.SIZE);
        int offsetSize = entriesSize * (Integer.SIZE / Byte.SIZE);
        int sizeSize = entriesSize * (Integer.SIZE / Byte.SIZE);
        return metaSize + minTimeSize + maxTimeSize + offsetSize + sizeSize;
    }
}
