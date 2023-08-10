package com.alibaba.lindorm.contest.v2.index;

import com.alibaba.lindorm.contest.common.Tuple;
import com.alibaba.lindorm.contest.common.TwoTuple;
import com.alibaba.lindorm.contest.memIndex.DataBlock;

import java.util.List;

public class DiskIndex {

    // vin + columnName 查找 offset
    // offset -> index entry list
    // list.forEach(item -> {
    //      item -> blockData
    // })

    /**
     *
     * @param tuples first: dataBlock, second: offset
     */
    public void put(List<TwoTuple<DataBlock, Long>> tuples) {

    }

    public long read(long key) {

        /*
            通过key 返回offset
         */
        return 0;
    }
}
