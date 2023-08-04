package com.alibaba.lindorm.contest.index;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.lindorm.contest.MemoryBufferExample;
import com.alibaba.lindorm.contest.common.TwoTuple;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-04
 */
public class MemoryBufferIndex {

    private Map<String, TwoTuple<Long, Long>> bufferIndex;

    private Map<Long, Map<String, Long>> diskIndex;

    public MemoryBufferIndex() {
        bufferIndex = new ConcurrentHashMap<>();
        diskIndex = new ConcurrentHashMap<>();
    }

    public Map<Long, Map<String, Long>> getDiskIndex() {
        return diskIndex;
    }

    public void setDiskIndex(Map<Long, Map<String, Long>> diskIndex) {
        this.diskIndex = diskIndex;
    }
}
