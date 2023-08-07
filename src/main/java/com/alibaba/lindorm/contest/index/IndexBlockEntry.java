package com.alibaba.lindorm.contest.index;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-05
 */
public class IndexBlockEntry {

    private long minTs;

    private long maxTs;

    private long offset;

    private int size;

    public IndexBlockEntry(long minTs, long maxTs, long offset, int size) {
        this.minTs = minTs;
        this.maxTs = maxTs;
        this.offset = offset;
        this.size = size;
    }

    public long getMinTs() {
        return minTs;
    }

    public long getMaxTs() {
        return maxTs;
    }

    public long getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }
}
