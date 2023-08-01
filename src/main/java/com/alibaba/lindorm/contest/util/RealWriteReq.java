package com.alibaba.lindorm.contest.util;

import java.io.Serializable;
import java.util.Collection;

import com.alibaba.lindorm.contest.structs.Constant;
import com.alibaba.lindorm.contest.structs.Row;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-01
 */
public class RealWriteReq implements Serializable {

    private int crc;

    private int pos;

    private long ts;

    private int vinOffset;

    private int keyOffset;

    private int valueOffset;

    private byte[] vin;

    private byte[] key;

    private byte[] valueType;

    private byte[] value;

    public int getCrc() {
        return crc;
    }

    public void setCrc(int crc) {
        this.crc = crc;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public int getVinOffset() {
        return vinOffset;
    }

    public void setVinOffset(int vinOffset) {
        this.vinOffset = vinOffset;
    }

    public int getKeyOffset() {
        return keyOffset;
    }

    public void setKeyOffset(int keyOffset) {
        this.keyOffset = keyOffset;
    }

    public int getValueOffset() {
        return valueOffset;
    }

    public void setValueOffset(int valueOffset) {
        this.valueOffset = valueOffset;
    }

    public byte[] getVin() {
        return vin;
    }

    public void setVin(byte[] vin) {
        this.vin = vin;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValueType() {
        return valueType;
    }

    public void setValueType(byte[] valueType) {
        this.valueType = valueType;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}
