package com.alibaba.lindorm.contest;

import static com.alibaba.lindorm.contest.Serialization.serialize;
import static com.alibaba.lindorm.contest.common.MoreRunnables.supplyWithFailSafe;

import java.io.File;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.lindorm.contest.common.MoreSupplizers;
import com.alibaba.lindorm.contest.structs.WriteRequest;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-01
 */
public final class AioCache {

    private LinkedBlockingQueue<WriteRequest> queue;
    private AsynchronousFileChannel fileChannel;

    // key: vin, value: pos
    private Map<String, Set<Integer>> vinPosMap;

    // key: column: value: pos
    private Map<String, Set<Integer>> columnPosMap;

    private Path filePath;

    private static volatile int fileOffset = 0;

    private final MoreSupplizers<Task> taskMoreSupplizers = MoreSupplizers.lazy(Task::new);

    public AioCache(Path path) {
        this.queue = new LinkedBlockingQueue<>();
        vinPosMap = new ConcurrentHashMap<>();
        columnPosMap = new ConcurrentHashMap<>();
        taskMoreSupplizers.get().execute(this::flushFromQueue);
        try {
            Path channelPath =
                    Path.of(path.toString() + File.separator + path.toString() + "_" + System.currentTimeMillis());
            fileChannel = AsynchronousFileChannel.open(channelPath, StandardOpenOption.WRITE, StandardOpenOption.READ,
                    StandardOpenOption.CREATE);
        } catch (Exception e) {

        }
    }

    public void put(WriteRequest data) {
        try {
            queue.put(data); // 将数据放入队列
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void flushFromQueue()  {
        try {
            if (!fileChannel.isOpen() || queue.peek() == null) {
                System.out.println("fileChanel is null");
                return;
            }
            WriteRequest request = queue.poll();
            ByteBuffer flushBuf = ByteBuffer.allocate(serialize(request).length);
            request.getRows().forEach(it -> {
                try {
                    // todo ts需要后续做索引，查最新ts
                    long ts = it.getTimestamp();

                    // 构造vin索引
                    String vin = String.valueOf(it.getVin());
                    Set<Integer> vinOffsetSet = vinPosMap.getOrDefault(vin, new HashSet<>());
                    int vinLen = serialize(it.getVin()).length;
                    int vinOffset = vinLen + fileOffset;
                    vinOffsetSet.add(fileOffset);
                    vinPosMap.put(vin, vinOffsetSet);


                    it.getColumns().forEach((k, v) -> {
                        try {
                            // 添加column索引
                            Set<Integer> columnOffsetSet = columnPosMap.getOrDefault(k, new HashSet<>());
                            columnOffsetSet.add(fileOffset);
                            columnPosMap.put(k, columnOffsetSet);

                            int keyLen = serialize(k).length;
                            int keyOffset = keyLen + vinOffset;
                            int valueLen = serialize(v).length;
                            int valueOffset = valueLen + keyOffset;

                            //  存储字节数据
                            ByteBuffer buffer = ByteBuffer.allocate(28 + vinLen + keyLen + valueLen);
                            buffer.putInt(1);
                            buffer.putInt(fileOffset);
                            buffer.putLong(ts);
                            buffer.putInt(vinOffset);
                            buffer.putInt(keyOffset);
                            buffer.putInt(valueOffset);
                            buffer.put(k.getBytes());
                            buffer.put(String.valueOf(v.getColumnType()).getBytes());
                            buffer.put(v.getStringValue());
                            flushBuf.put(buffer);

                            fileOffset = valueOffset;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            System.out.println("azh");
            flushBuf.flip();
            System.out.println("flushBuf: "+flushBuf.position());
            fileChannel.write(flushBuf, fileOffset);
//            fileChannel.write(flushBuf, fileOffset, flushBuf, new CompletionHandler<Integer, ByteBuffer>() {
//                @Override
//                public void completed(Integer result, ByteBuffer attachment) {
//                    if (queue.peek() != null) {
//                        flushFromQueue();
//                    }
//                }
//
//                @Override
//                public void failed(Throwable exc, ByteBuffer attachment) {
//                    // 写入失败
//                }
//            });
        } catch (IOException e)  {
            // todo 日志处理
            e.printStackTrace();
        }
    }

    public LinkedBlockingQueue<WriteRequest> getQueue() {
        return queue;
    }

    public void flush() {
        flushFromQueue();
    }

//    private int writeRequestLen(WriteRequest request) {
//        int len = request.getTableName().getBytes().length;
//    }
}
