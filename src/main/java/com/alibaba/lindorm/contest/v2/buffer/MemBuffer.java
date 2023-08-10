package com.alibaba.lindorm.contest.v2.buffer;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.structs.WriteRequest;

import java.nio.ByteBuffer;
import java.util.List;

import static com.alibaba.lindorm.contest.common.MoreSupplizers.lazy;

public class MemBuffer {

    private ByteBuffer bufferA = ByteBuffer.allocateDirect(1024 * 4);

    private ByteBuffer bufferB = ByteBuffer.allocateDirect(1024 * 4);


    public void put(String tableName, Row row) {
        /*
            todo
            1. row 刷盘到buffer成为字节数组
            2. 更新 MemBuffer.put方法更新索引
            3. 当buffer写满了后需要刷磁盘
                3.1 compaction
                    3.1.2 刷新数据到磁盘 (data-block)
                        3.1.2.1 写disk-Buffer (index-entry)(size)
                    3.1.1 刷新索引到磁盘 index-entry
         */
    }

    public void read(String tableName, List<Vin> vinList, List<String> columnName) {
        /*
            todo
            1. 通过读cacheIndex获取vin+column的内存offset
            2. 通过offset从memBuffer上面查询具体的 List<byte[]> 值
            3. 判断条件
                3.1 如果是`executeLatestQuery` 接口
                    3.1.2 能够查到直接返回， else 查磁盘索引 -> 查磁盘数据返回
         */
    }

    public void range(String tableName, Vin vin, List<String> columnName, long startTime, long endTime) {
        /*
             3.2 如果是`executeTimeRangeQuery` 接口
                    3.2.1 如果ts范围在内存覆盖，就查内存直接返回
                    3.2.2 如果ts范围在内存和磁盘，那就需要查磁盘和内存组合返回
                    3.3.3 如果ts范围在磁盘，就直接查磁盘返回
         */
    }

    /**
     * 构建磁盘索引
     */
    public void load() {
        /*
            todo
            1. 读磁盘接口，扫所有磁盘数据 4->18->50
            2. 更新disk索引
         */
    }
}
