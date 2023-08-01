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
import java.util.ArrayList;

import com.alibaba.lindorm.contest.common.MoreSupplizers;
import com.alibaba.lindorm.contest.structs.LatestQueryRequest;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Schema;
import com.alibaba.lindorm.contest.structs.TimeRangeQueryRequest;
import com.alibaba.lindorm.contest.structs.WriteRequest;

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
//        cache.flush();
        runOnceSilently(() -> {
            // todo 强制刷盘 优雅停机
            isConnected = false;
        });

    }

    @Override
    public void upsert(WriteRequest wReq) throws IOException {
        cache.put(wReq);
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
}
