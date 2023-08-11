package com.alibaba.lindorm.contest.util;

import com.alibaba.lindorm.contest.pojo.data.DataBlock;
import com.alibaba.lindorm.contest.pojo.index.IndexBlock;
import com.alibaba.lindorm.contest.pojo.index.IndexBlockEntry;
import com.alibaba.lindorm.contest.pojo.index.IndexBlockMeta;
import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 反序列化工具类
 */
public class DeserializationUtil {
    public static DataBlock deserializationDataBlock(ByteBuffer buffer, int type) {
        DataBlock dataBlock = new DataBlock();
        dataBlock.setType(buffer.getInt());
        dataBlock.setLength(buffer.getInt());
        List<Long> timestamps = new ArrayList<>();
        for (int i = 0; i < dataBlock.getLength(); i++) {
            timestamps.add(buffer.getLong());
        }
        dataBlock.setTimestamps(timestamps);
        List<ColumnValue> columnValues = new ArrayList<>();
        for (int i = 0; i < dataBlock.getLength(); i++) {
            if (type == 0) {
                columnValues.add(new ColumnValue.IntegerColumn(buffer.getInt()));
            } else if (type == 1) {
                columnValues.add(new ColumnValue.DoubleFloatColumn(buffer.getDouble()));
            } else {
                int valueLen = buffer.getInt();
                byte[] bytes = new byte[valueLen];
                buffer.get(bytes);
                columnValues.add(new ColumnValue.StringColumn(ByteBuffer.wrap(bytes)));
            }
        }
        dataBlock.setColumnValues(columnValues);
        return dataBlock;
    }

    public static IndexBlock deSerializationIndexBlock(ByteBuffer buffer) {
        IndexBlock indexBlock = new IndexBlock();

        IndexBlockMeta meta = new IndexBlockMeta();
        meta.setKeyLength(buffer.getLong());
        byte[] bytes = new byte[(int) meta.getKeyLength()];
        buffer.get(bytes);
        meta.setKey(new String(bytes));
        int type = buffer.getInt();
        if (type == 0) {
            meta.setType(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
        } else if (type == 1) {
            meta.setType(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
        } else {
            meta.setType(ColumnValue.ColumnType.COLUMN_TYPE_STRING);
        }
        meta.setCount(buffer.getInt());

        List<IndexBlockEntry> entries = new ArrayList<>();
        for (int i = 0; i < meta.getCount(); i++) {
            IndexBlockEntry entry = new IndexBlockEntry();
            entry.setMinTime(buffer.getLong());
            entry.setMaxTime(buffer.getLong());
            entry.setOffset(buffer.getInt());
            entry.setSize(buffer.getInt());
            entries.add(entry);
        }

        indexBlock.setMeta(meta);
        indexBlock.setEntries(entries);
        return indexBlock;
    }

    public static Long serializationFooter() {
        return 0L;
    }
}
