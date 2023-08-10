package com.alibaba.lindorm.contest.v2.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CacheIndex {

    private Map<String, List<Long>> bufferIndex;


    public void put(String key, long offset) {
//        List<Long> offsetList = bufferIndex.getOrDefault(key, new ArrayList<>());
//        offsetList.add(offset);
//        bufferIndex.put(key, offsetList);
        // todo buffer索引
    }

    public List<Long> read(String key) {

//        new List<>().stream().findFirst().or(0);

        // todo buffer索引
        return null;
    }
}
