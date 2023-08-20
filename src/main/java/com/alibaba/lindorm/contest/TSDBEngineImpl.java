//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.store.Store;
import com.alibaba.lindorm.contest.structs.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

public class TSDBEngineImpl extends TSDBEngine {

  private Store store;

  private void init() {
    this.store = new Store();
  }

  /**
   * This constructor's function signature should not be modified.
   * Our evaluation program will call this constructor.
   * The function's body can be modified.
   */
  public TSDBEngineImpl(File dataPath) {
    super(dataPath);
    init();
  }

  @Override
  public void connect() throws IOException {

  }

  private String getTablePath(String tableName) {
    return dataPath + "/" + tableName + ".txt";
  }

  @Override
  public void createTable(String tableName, Schema schema) throws IOException {
    store.createTable(getTablePath(tableName), schema);
  }

  @Override
  public void shutdown() {
    store.shutdown();
  }

  @Override
  public void upsert(WriteRequest wReq) throws IOException {
    String tableName = wReq.getTableName();
    Collection<Row> rows = wReq.getRows();
    store.insert(getTablePath(tableName), rows);
  }

  @Override
  public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
    String tableName = pReadReq.getTableName();
    Collection<Vin> vins = pReadReq.getVins();
    Set<String> requestedColumns = pReadReq.getRequestedColumns();
    return store.queryLatestData(getTablePath(tableName), vins, requestedColumns);
  }

  @Override
  public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
    String tableName = trReadReq.getTableName();
    Vin vin = trReadReq.getVin();
    long timeLowerBound = trReadReq.getTimeLowerBound();
    long timeUpperBound = trReadReq.getTimeUpperBound();
    Set<String> requestedColumns = trReadReq.getRequestedColumns();
    return store.queryTimeRangeData(getTablePath(tableName), vin, timeLowerBound, timeUpperBound, requestedColumns);
  }
}
