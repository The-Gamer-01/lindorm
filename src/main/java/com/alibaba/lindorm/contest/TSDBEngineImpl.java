//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;


import static com.alibaba.lindorm.contest.common.MoreRunnables.runOnceSilently;
import static com.alibaba.lindorm.contest.common.Preconditions.checkFileState;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.lindorm.contest.common.MoreSupplizers;
import com.alibaba.lindorm.contest.structs.LatestQueryRequest;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Schema;
import com.alibaba.lindorm.contest.structs.TimeRangeQueryRequest;
import com.alibaba.lindorm.contest.structs.WriteRequest;
import com.alibaba.lindorm.contest.util.RealWriteReq;

public class TSDBEngineImpl extends TSDBEngine {

    private static final String DATA_PATH = "data";

    private static final String INDEX_SUFFIX = ".json";

    private static boolean isConnected = false;

    private AioCache cache;

    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    public TSDBEngineImpl(File dataPath) {
        super(dataPath);
    }

    @Override
    public void connect() throws IOException {
        if (isConnected) {
            throw new IOException("The current database is already linked.");
        }
        if (dataPath == null) {
            throw new IOException("The current database does not exist.");
        }
        if (!dataPath.exists()) {
            checkFileState(dataPath.mkdir(), "Description Failed to execute createNewFile.");
        }
        // todo 从文件加载数据构建索引
        isConnected = true;
        cache = new AioCache(dataPath.toPath());
    }

    @Override
    public void createTable(String tableName, Schema schema) throws IOException {
        File tableFile = new File(dataPath, String.join("_", tableName, DATA_PATH));
        if (tableFile.exists()) {
            throw new IOException("Table " + tableName + " has been created.");
        }
        checkFileState(tableFile.mkdir(), "Description Failed to execute createNewFile. fileName: " + tableName);
    }

    @Override
    public void shutdown() {
        if (!isConnected) {
            return;
        }
        // 调试用
        cache.flush();
        runOnceSilently(() -> {
            // todo 强制刷盘 优雅停机
            isConnected = false;
        });

    }

    @Override
    public void upsert(WriteRequest wReq) throws IOException {
        cache.put(getRealWriteReq(wReq));
    }

    @Override
    public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
        return null;
    }

    @Override
    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
        return null;
    }

    public AioCache getCache() {
        return cache;
    }

    public static List<RealWriteReq> getRealWriteReq(WriteRequest wReq) {
        List<RealWriteReq> list = new ArrayList<>();
        wReq.getRows().forEach(row -> {
            row.getColumns().forEach((k, v) -> {
                RealWriteReq req = new RealWriteReq();
                req.setTs(row.getTimestamp());
                req.setVin(row.getVin().getVin());
                req.setKey(k.getBytes(StandardCharsets.UTF_8));
                req.setValueType(v.getColumnType().name().getBytes(StandardCharsets.UTF_8));
                switch (v.getColumnType()) {
                    case COLUMN_TYPE_STRING:
                        req.setValue(v.getStringValue().array());
                        break;
                    case COLUMN_TYPE_INTEGER:
                        byte[] data = String.valueOf(v.getIntegerValue()).getBytes();
                        ByteBuffer buffer = ByteBuffer.allocate(data.length);
                        buffer.put(data);
                        req.setValue(buffer.array());
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        byte[] data2 = String.valueOf(v.getDoubleFloatValue()).getBytes();
                        ByteBuffer buffer2 = ByteBuffer.allocate(data2.length);
                        buffer2.put(data2);
                        req.setValue(buffer2.array());
                        break;
                }
                list.add(req);
            });

        });
        return list;
    }
}
