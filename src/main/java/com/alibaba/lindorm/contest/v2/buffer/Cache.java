package com.alibaba.lindorm.contest.v2.buffer;

import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

public interface Cache {

    void write(String tableName, Row row);

    void flush();

    ArrayList<Row> getLatestRowsByVins(String tableName, Collection<Vin> vins, Set<String> columns);
}
