package com.alibaba.lindorm.contest.pojo.data;

import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.util.List;
import java.util.Objects;

public class DataBlock {

    private Integer type;

    private Integer length;

    private List<Long> timestamps;

    private List<ColumnValue> columnValues;

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public List<Long> getTimestamps() {
        return timestamps;
    }

    public void setTimestamps(List<Long> timestamps) {
        this.timestamps = timestamps;
    }

    public List<ColumnValue> getColumnValues() {
        return columnValues;
    }

    public void setColumnValues(List<ColumnValue> columnValues) {
        this.columnValues = columnValues;
    }

    @Override
    public String toString() {
        return "DataBlock{" +
                "type=" + type +
                ", length=" + length +
                ", timestamps=" + timestamps +
                ", columnValues=" + columnValues +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataBlock dataBlock = (DataBlock) o;
        return Objects.equals(type, dataBlock.type) && Objects.equals(length, dataBlock.length) && Objects.equals(timestamps, dataBlock.timestamps) && Objects.equals(columnValues, dataBlock.columnValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, length, timestamps, columnValues);
    }
}
