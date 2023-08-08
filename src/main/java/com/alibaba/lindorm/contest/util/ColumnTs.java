package com.alibaba.lindorm.contest.util;

import com.alibaba.lindorm.contest.structs.ColumnValue;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-05
 */
public class ColumnTs implements Comparable<ColumnTs> {

    private long timestamp;

    private byte[] columnValue;

    public ColumnTs(long timestamp, byte[] columnValue) {
        this.timestamp = timestamp;
        this.columnValue = columnValue;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public byte[] getColumnValue() {
        return columnValue;
    }

    public void setColumnValue(byte[] columnValue) {
        this.columnValue = columnValue;
    }

    @Override
    public int compareTo(ColumnTs o) {
        return (int) (o.getTimestamp() - this.timestamp);
    }
}
