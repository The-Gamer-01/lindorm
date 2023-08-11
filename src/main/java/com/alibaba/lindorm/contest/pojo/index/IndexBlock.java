package com.alibaba.lindorm.contest.pojo.index;

import java.util.List;

public class IndexBlock implements Comparable<IndexBlockMeta> {

    private IndexBlockMeta meta;

    private List<IndexBlockEntry> entries;

    public void setEntries(List<IndexBlockEntry> entries) {
        this.entries = entries;
    }

    public void setMeta(IndexBlockMeta meta) {
        this.meta = meta;
    }

    public IndexBlockMeta getMeta() {
        return meta;
    }

    public List<IndexBlockEntry> getEntries() {
        return entries;
    }

    @Override
    public int compareTo(IndexBlockMeta o) {
        return o.getKey().compareTo(meta.getKey());
    }

    @Override
    public String toString() {
        return "IndexBlock{" +
                "meta=" + meta +
                ", entries=" + entries +
                '}';
    }
}
