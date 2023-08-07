package com.alibaba.lindorm.contest.memIndex;

import com.alibaba.lindorm.contest.common.TwoTuple;
import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class IndexBlock {
    static final long timeStampInterval = 1000;     // 1000ms = 1s
    IndexBlockMetaData metaData;
    List<IndexBlockEntry> entryList;
    ReadWriteLock lock = new ReentrantReadWriteLock();

    public IndexBlock(String seriesKey, String fieldKey, ColumnValue.ColumnType columnType) {
        metaData = new IndexBlockMetaData(seriesKey, fieldKey, IndexBlockMetaData.formKey(seriesKey, fieldKey), columnType);
        // todo: lazy load
        this.entryList = new ArrayList<>();
    }

    public static String formSeriesKey(String tableName, String tagKey) {
        return tableName+ "_" + tagKey;
    }

    public String getKey() {
        return this.metaData.getKey();
    }

    public int addData(TwoTuple<Long, ColumnValue> value) {
        assert this.metaData.columnType.equals(value.getSecond().getColumnType());
        long minTimeStamp = value.getFirst() - value.getFirst()%timeStampInterval;
        long maxTimeStamp = minTimeStamp + timeStampInterval;
        this.lock.writeLock().lock();
        IndexBlockEntry entry = null;
        try {
            int i = 0;
            for (; i < this.entryList.size(); i++) {
                if (entryList.get(i).getMaxTime() == maxTimeStamp || entryList.get(i).getMinTime() >= maxTimeStamp ) {
                    break;
                }
            }
            if (i == this.entryList.size() || this.entryList.get(i).getMaxTime() != maxTimeStamp) {
                entry = new IndexBlockEntry(minTimeStamp, maxTimeStamp);
                entryList.add(i, entry);
            } else {
                entry = entryList.get(i);
            }
        } finally {
            this.lock.writeLock().unlock();
        }
        return entry.addValue(value);
    }

    public Optional<ColumnValue> getData(long timeStamp) {
        this.lock.readLock().lock();
        IndexBlockEntry entry = null;
        try {
            // todo: 二分查找
            for (int i = 0 ; i < this.entryList.size(); i++) {
                if (entryList.get(i).getMinTime() > timeStamp) {
                    break;
                }
                if (timeStamp < entryList.get(i).getMaxTime() && entryList.get(i).getMinTime() <= timeStamp) {
                    entry = entryList.get(i);
                    break;
                }
            }
        } finally {
            this.lock.readLock().unlock();
        }
        if (entry == null) {
            return Optional.empty();
        }
        Optional<TwoTuple<Long, ColumnValue>> value = entry.searchValue(timeStamp);
        if (value.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(value.get().second);
        }
    }

    public Optional<ColumnValue> getLatestValue() {
        this.lock.readLock().lock();
        try {
            if (this.entryList.size() == 0) {
                return Optional.empty();
            }
            Optional<TwoTuple<Long, ColumnValue>> latestValue = this.entryList.get(this.entryList.size()-1).getLatestValue();
            if (latestValue.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(latestValue.get().getSecond());
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public List<ColumnValue> rangeGetColumnValue(long minTimeStamp, long maxTimeStamp) {
        this.lock.readLock().lock();
        try {
            List<ColumnValue> result = new ArrayList<>();
            if (this.entryList.size() == 0) {
               return result;
            }
            // todo: 二分查找
            // todo: 并发
            for (int i = 0; i < this.entryList.size(); i++) {
                IndexBlockEntry curEntry = this.entryList.get(i);
                if (curEntry.getMinTime() <= maxTimeStamp || curEntry.getMaxTime() > minTimeStamp) {
                    result.addAll(curEntry.rangeGetColumnValue(minTimeStamp, maxTimeStamp));
                }
                if (curEntry.getMaxTime() <= minTimeStamp) {
                    break;
                }
            }
            return result;
        } finally {
           this.lock.readLock().unlock();
        }
    }
}
