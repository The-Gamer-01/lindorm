package com.alibaba.lindorm.contest.memIndex;

import com.alibaba.lindorm.contest.common.Tuple;
import com.alibaba.lindorm.contest.common.TwoTuple;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemIndex implements IMemIndex{
    private List<IndexBlock> indexBlockList;
    private static final byte[] vinBytes = "vin".getBytes();
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    public MemIndex() {
        indexBlockList = new ArrayList<>();
    }

    @Override
    public void addRow(String tableName, Row row) {
        Tag[] tags = new Tag[]{new Tag(vinBytes, row.getVin().getVin())};
        String tagKey = TagSet.formTagKey(tags);
        String seriesKey = IndexBlock.formSeriesKey(tableName, tagKey);
        for(Map.Entry<String, ColumnValue> column : row.getColumns().entrySet()) {
            String fieldKey = column.getKey();
            ColumnValue columnValue = column.getValue();
            String key = IndexBlockMetaData.formKey(seriesKey, fieldKey);
            IndexBlock indexBlock = null;
            lock.writeLock().lock();
            try {
                // todo: 二分查找
                int i = 0;
                for (; i < indexBlockList.size(); i++) {
                    int cmp = indexBlockList.get(i).getKey().compareTo(key);
                    if (cmp >= 0) {
                        break;
                    }
                }
                if (i == indexBlockList.size() || indexBlockList.get(i).getKey().compareTo(key) != 0) {
                    indexBlock = new IndexBlock(seriesKey, fieldKey, columnValue.getColumnType());
                    this.indexBlockList.add(i, indexBlock);
                } else {
                    indexBlock = indexBlockList.get(i);
                }
            } finally {
                lock.writeLock().unlock();
            }
            indexBlock.addData(new TwoTuple<>(row.getTimestamp(), columnValue));
        }
    }

    @Override
    public Optional<ColumnValue> getLatestColumnValue(String tableName, Vin vin, String columnFieldName) {
        Tag[] tags = new Tag[]{new Tag(vinBytes, vin.getVin())};
        String key = IndexBlockMetaData.formKey(IndexBlock.formSeriesKey(tableName, TagSet.formTagKey(tags)), columnFieldName);
        lock.readLock().lock();
        try {
            Optional<IndexBlock> indexBlock = getIndexBlockByKey(key);
            if (indexBlock.isEmpty()) {
                return Optional.empty();
            }
            return indexBlock.get().getLatestValue();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<ColumnValue> rangeGetColumnValue(String tableName, Vin vin, String columnFieldName, long minTs, long maxTs) {
        Tag[] tags = new Tag[]{new Tag(vinBytes, vin.getVin())};
        String key = IndexBlockMetaData.formKey(IndexBlock.formSeriesKey(tableName, TagSet.formTagKey(tags)), columnFieldName);
        Optional<IndexBlock> indexBlock = getIndexBlockByKey(key);
        if (indexBlock.isEmpty()) {
            return new ArrayList<>();
        }
        return indexBlock.get().rangeGetColumnValue(minTs, maxTs);
    }

    private Optional<IndexBlock> getIndexBlockByKey(String key) {
        // todo: 二分查找
        IndexBlock indexBlock = null;
        for (int i = 0; i < this.indexBlockList.size(); i++) {
            IndexBlock curBlock = this.indexBlockList.get(i);
            int cmp = curBlock.getKey().compareTo(key);
            if (cmp == 0) {
                indexBlock = curBlock;
                break;
            }
            if (cmp > 0) {
                break;
            }
        }
        if (indexBlock == null) {
            return Optional.empty();
        }
        return Optional.of(indexBlock);
    }
}
