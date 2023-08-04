package com.alibaba.lindorm.contest.common;

import static com.alibaba.lindorm.contest.structs.ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT;
import static com.alibaba.lindorm.contest.structs.ColumnValue.ColumnType.COLUMN_TYPE_INTEGER;
import static com.alibaba.lindorm.contest.structs.ColumnValue.ColumnType.COLUMN_TYPE_STRING;

import com.alibaba.lindorm.contest.structs.ColumnValue.ColumnType;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-04
 */
public class TypeUtils {

    public static int columnTypeOf(ColumnType type) {
        switch (type) {
            case COLUMN_TYPE_STRING:
                return 0;
            case COLUMN_TYPE_INTEGER:
                return 1;
            case COLUMN_TYPE_DOUBLE_FLOAT:
                return 2;
            default:
                return -1;
        }
    }

    public static ColumnType columnTypeValue(int n) {
        switch (n) {
            case 0:
                return COLUMN_TYPE_STRING;
            case 1:
                return COLUMN_TYPE_INTEGER;
            case 2:
                return COLUMN_TYPE_DOUBLE_FLOAT;
            default:
                return COLUMN_TYPE_STRING;
        }

    }
}
