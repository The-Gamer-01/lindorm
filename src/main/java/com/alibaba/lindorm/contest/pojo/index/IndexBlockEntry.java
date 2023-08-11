package com.alibaba.lindorm.contest.pojo.index;

public class IndexBlockEntry {
    private long minTime;

    private long maxTime;

    private int offset;

    private int size;

    public long getMinTime() {
        return minTime;
    }

    public void setMinTime(long minTime) {
        this.minTime = minTime;
    }

    public long getMaxTime() {
        return maxTime;
    }

    public void setMaxTime(long maxTime) {
        this.maxTime = maxTime;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return "IndexBlockEntry{" +
                "minTime=" + minTime +
                ", maxTime=" + maxTime +
                ", offset=" + offset +
                ", size=" + size +
                '}';
    }
}
