package com.alibaba.lindorm.contest.memIndex;

import com.alibaba.lindorm.contest.structs.ColumnValue;

public class IndexBlockMetaData {
    String seriesKey;
    String fieldKey;
    ColumnValue.ColumnType columnType;

    private String key;

    public IndexBlockMetaData(String _seriesKey, String _fieldKey, String _key, ColumnValue.ColumnType _columnType) {
        this.seriesKey = _seriesKey;
        this.fieldKey = _fieldKey;
        this.columnType = _columnType;
        this.key = _key;
    }

    public String getKey() {
        return key;
    }

    public static String formKey(String seriesKey, String fieldKey) {
        return seriesKey + "_" + fieldKey;
    }
}
