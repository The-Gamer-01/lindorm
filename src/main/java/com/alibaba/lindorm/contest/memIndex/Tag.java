package com.alibaba.lindorm.contest.memIndex;

public class Tag {
    byte[] key;
    byte[] value;

    public Tag(byte[] _key, byte[] _value) {
        this.key = _key;
        this.value = _value;
    }
}
