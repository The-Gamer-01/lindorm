import com.alibaba.lindorm.contest.TSDBEngine;
import com.alibaba.lindorm.contest.TSDBEngineImpl;
import com.alibaba.lindorm.contest.structs.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TSDBEngineTest {
    public static void main(String[] args) throws IOException {
        String tableName = "users";
        TSDBEngine tsdbEngine = new TSDBEngineImpl(new File("data_dir"));
        tsdbEngine.connect();
        tsdbEngine.createTable(tableName, null);
        List<Row> rows1 = new ArrayList<>();
        long row1Time = System.currentTimeMillis();
        Row row1 = new Row(new Vin(new byte[]{'a'}), System.currentTimeMillis(), Map.of("column1", new ColumnValue.StringColumn(ByteBuffer.wrap("v3".getBytes()))));
        System.out.println(row1);
        rows1.add(row1);
        WriteRequest writeRequest = new WriteRequest(tableName, rows1);
        tsdbEngine.upsert(writeRequest);
        tsdbEngine.shutdown();

        tsdbEngine.connect();
        LatestQueryRequest latestQueryRequest = new LatestQueryRequest(tableName, List.of(new Vin(new byte[]{'a'})), Set.of("column1"));
        ArrayList<Row> rows2 = tsdbEngine.executeLatestQuery(latestQueryRequest);
        System.out.println(rows2);

        List<Row> rows3 = new ArrayList<>();
        Row row2 = new Row(new Vin(new byte[]{'a'}), row1Time, Map.of("column1", new ColumnValue.StringColumn(ByteBuffer.wrap("v323232".getBytes()))));
        rows3.add(row2);
        WriteRequest writeRequest2 = new WriteRequest(tableName, rows3);
        tsdbEngine.upsert(writeRequest2);
        ArrayList<Row> rows4 = tsdbEngine.executeLatestQuery(latestQueryRequest);
        System.out.println("rows4:" + rows4);

        tsdbEngine.shutdown();

        tsdbEngine.connect();
        TimeRangeQueryRequest timeRangeQueryRequest = new TimeRangeQueryRequest(tableName, new Vin(new byte[]{'a'}), Set.of("column1"), 1692469900630L, 1692469900632L);
        ArrayList<Row> rows5 = tsdbEngine.executeTimeRangeQuery(timeRangeQueryRequest);
        System.out.println(rows5);
        tsdbEngine.shutdown();
    }
}
