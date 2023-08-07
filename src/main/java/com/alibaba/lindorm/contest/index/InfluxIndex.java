package com.alibaba.lindorm.contest.index;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.lindorm.contest.common.TwoTuple;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-05
 */
public class InfluxIndex {

    private Map<String, IndexBlockEntry> bufferIndex;


    public InfluxIndex() {
        this.bufferIndex = new ConcurrentHashMap<>();
    }

    public Map<String, IndexBlockEntry> getBufferIndex() {
        return bufferIndex;
    }

    public void setBufferIndex(Map<String, IndexBlockEntry> bufferIndex) {
        this.bufferIndex = bufferIndex;
    }
}
