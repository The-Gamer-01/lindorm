package com.alibaba.lindorm.contest.memIndex;

import com.alibaba.lindorm.contest.common.TwoTuple;
import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataBlock {
    List<TwoTuple<Long, ColumnValue>> values;
    ReadWriteLock lock = new ReentrantReadWriteLock();

    public DataBlock() {
        // todo: 懒加载
        values = new ArrayList<>();
    }

    public int addValue(TwoTuple<Long, ColumnValue> value) {
        this.lock.writeLock().lock();
        try {
            // 找大于等于的第一个数
            // todo: 二分查找
            int i = 0;
            for (; i < this.values.size(); i++) {
                if (this.values.get(i).getFirst() >= value.getFirst()) {
                    break;
                }
            }
            if (i != this.values.size() && this.values.get(i).getFirst().equals(value.getFirst())) {
                this.values.set(i, value);
            } else {
                this.values.add(i, value);
            }
            return i;
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public Optional<TwoTuple<Long, ColumnValue>> searchValue (long timeStamp) {
        this.lock.readLock().lock();
        try {
            // todo: 二分查找
            for(int i = 0; i < this.values.size(); i++) {
                if (this.values.get(i).getFirst().equals(timeStamp)) {
                    return Optional.of(this.values.get(i));
                }
            }
            return Optional.empty();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public Optional<TwoTuple<Long, ColumnValue>> getLatestValue() {
        this.lock.readLock().lock();
        try {
            return Optional.of(this.values.get(this.values.size()-1));
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public List<ColumnValue> rangeGetColumnValue(long minTimeStamp, long maxTimeStamp) {
        this.lock.readLock().lock();
        try {
            List<ColumnValue> result = new ArrayList<>();
            // todo: 二分查找
            for (int i = 0; i < this.values.size(); i++) {
                TwoTuple<Long, ColumnValue> curValue = this.values.get(i);
                if (curValue.getFirst() >= minTimeStamp && curValue.getFirst() <= maxTimeStamp) {
                    result.add(curValue.getSecond());
                }
                if (curValue.getFirst() >= maxTimeStamp) {
                    break;
                }
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }
}
