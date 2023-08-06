package com.alibaba.lindorm.contest.memIndex;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.util.List;
import java.util.Optional;

public interface IMemIndex {
    public void addRow(String tableName, Row row);

    public Optional<ColumnValue> getLatestColumnValue(String tableName, Vin vin, String columnFieldName);

    // 查询[minTs, maxTs]范围内column的Value
    public List<ColumnValue> rangeGetColumnValue(String tableName, Vin vin, String columnFieldName, long minTs, long maxTs);
}
