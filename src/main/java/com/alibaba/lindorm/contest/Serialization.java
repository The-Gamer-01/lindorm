package com.alibaba.lindorm.contest;

import static com.alibaba.lindorm.contest.structs.ColumnValue.ColumnType.COLUMN_TYPE_STRING;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.structs.WriteRequest;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-01
 * 不支持序列化
 */
public class Serialization {

    public static byte[] serialize(Object obj) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
            return baos.toByteArray();
        }
    }

    public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return is.readObject();
    }


    public static byte[] serializeInWriteRequest(WriteRequest request) {
        return null;
    }

    public static byte[] serializeInRow(Row row) {
        List<Byte> list = new ArrayList<>();
        byte[] vb = serializeInVin(row.getVin());

        long ts = row.getTimestamp();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(ts);
        byte[] tsb = buffer.array();

        return null;
    }

    public static byte[] serializeInVin(Vin vin) {
        return vin.getVin();
    }

    public static byte[] serializeInColumnValue(ColumnValue value) {
        switch (value.getColumnType()) {
            case COLUMN_TYPE_STRING:
                byte[] bs1 = "COLUMN_TYPE_STRING".getBytes();
                byte[] bs2 = value.getStringValue().array();
                return mergeArrays(bs1, bs2);
            case COLUMN_TYPE_INTEGER:
                byte[] bi1 = "COLUMN_TYPE_INTEGER".getBytes();
                int i2 = value.getIntegerValue();
                ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
                buffer.putInt(i2);
                return mergeArrays(bi1, buffer.array());
            case COLUMN_TYPE_DOUBLE_FLOAT:
                byte[] bf1 = "COLUMN_TYPE_DOUBLE_FLOAT".getBytes();
                double f2 = value.getDoubleFloatValue();
                ByteBuffer fbuf = ByteBuffer.allocate(Double.BYTES);
                fbuf.putDouble(f2);
                return mergeArrays(bf1, fbuf.array());
            default:
                break;
        }
        return null;
    }

    private static byte[] mergeArrays(byte[] array1, byte[] array2) {
        byte[] mergedArray = new byte[array1.length + array2.length];
        System.arraycopy(array1, 0, mergedArray, 0, array1.length);
        System.arraycopy(array2, 0, mergedArray, array1.length, array2.length);
        return mergedArray;
    }
}
