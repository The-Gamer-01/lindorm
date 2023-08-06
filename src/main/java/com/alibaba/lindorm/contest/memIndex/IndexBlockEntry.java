package com.alibaba.lindorm.contest.memIndex;

import com.alibaba.lindorm.contest.common.TwoTuple;
import com.alibaba.lindorm.contest.structs.ColumnValue;

import javax.xml.crypto.Data;
import java.util.List;
import java.util.Optional;

//[minTime, maxTime)，左闭右开
public class IndexBlockEntry {
    private long minTime;
    private long maxTime;
    private DataBlock dataBlock;

    public IndexBlockEntry(long minTime, long maxTime) {
        this.minTime = minTime;
        this.maxTime = maxTime;
        // todo: 改成sync.once
        dataBlock = new DataBlock();
    }

    public long getMinTime() {
        return minTime;
    }

    public long getMaxTime() {
        return maxTime;
    }

    public int addValue(TwoTuple<Long, ColumnValue> value) {
        return dataBlock.addValue(value);
    }

    public Optional<TwoTuple<Long, ColumnValue>> searchValue(long timeStamp) {
        return dataBlock.searchValue(timeStamp);
    }

    public Optional<TwoTuple<Long, ColumnValue>> getLatestValue() {
        return dataBlock.getLatestValue();
    }

    public List<ColumnValue> rangeGetColumnValue(long minTimeStamp, long maxTimeStamp) {
        return dataBlock.rangeGetColumnValue(Math.max(minTimeStamp, this.minTime), Math.min(maxTimeStamp, this.maxTime));
    }
}
