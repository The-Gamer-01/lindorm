package com.alibaba.lindorm.contest.pojo.index;

import com.alibaba.lindorm.contest.structs.ColumnValue;

public class IndexBlockMeta {

    private long keyLength;

    private String key;

    private ColumnValue.ColumnType type;

    private int count;

    public long getKeyLength() {
        return keyLength;
    }

    public void setKeyLength(long keyLength) {
        this.keyLength = keyLength;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getType() {
        if (type == ColumnValue.ColumnType.COLUMN_TYPE_INTEGER) {
            return 0;
        } else if (type == ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT) {
            return 1;
        } else if (type == ColumnValue.ColumnType.COLUMN_TYPE_STRING) {
            return 2;
        }
        return -1;
    }

    public void setType(ColumnValue.ColumnType type) {
        this.type = type;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "IndexBlockMeta{" +
                "keyLength=" + keyLength +
                ", key='" + key + '\'' +
                ", type=" + type +
                ", count=" + count +
                '}';
    }
}
