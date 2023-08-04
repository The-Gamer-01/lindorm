package com.alibaba.lindorm.contest;

import static com.alibaba.lindorm.contest.Serialization.serialize;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.lindorm.contest.common.MoreSupplizers;
import com.alibaba.lindorm.contest.storage.FileStorage;
import com.alibaba.lindorm.contest.util.RealWriteReq;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-01
 */
public final class AioCache {

    private LinkedBlockingQueue<List<RealWriteReq>> queue;
    private AsynchronousFileChannel fileChannel;

    private FileStorage store;

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
        //        taskMoreSupplizers.get().execute(this::flushFromQueue);
        try {

            Path channelPath =
                    Path.of(path.toString() + File.separator + path.toString() + "_" + System.currentTimeMillis());

            fileChannel = AsynchronousFileChannel.open(channelPath, StandardOpenOption.WRITE, StandardOpenOption.READ,
                    StandardOpenOption.CREATE);
            FileStorage storage = new FileStorage();
            storage.init(path.toString());
        } catch (Exception e) {

        }
    }

    public void put(List<RealWriteReq> data) {
        try {
            queue.put(data); // 将数据放入队列
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public List<RealWriteReq> read() {
        return null;
    }

    private void flushFromQueue() {
        try {
            if (!fileChannel.isOpen() || queue.peek() == null) {
                System.out.println("fileChanel is null");
                return;
            }
            List<RealWriteReq> request = queue.poll();

            ByteBuffer flushBuf = ByteBuffer.allocate(serialize(request).length);
            request.forEach(it -> {
                    // todo ts需要后续做索引，查最新ts
                    long ts = it.getTs();

                    // 构造vin索引
                    String vin = Arrays.toString(it.getVin());
                    Set<Integer> vinOffsetSet = vinPosMap.getOrDefault(vin, new HashSet<>());
                    int vinLen = vin.getBytes(StandardCharsets.UTF_8).length;
                    int vinOffset = vinLen + fileOffset;
                    vinOffsetSet.add(fileOffset);
                    vinPosMap.put(vin, vinOffsetSet);

                    byte[] key = it.getKey();
                    byte[] value = it.getValue();

                    Set<Integer> columnOffsetSet =
                            columnPosMap.getOrDefault(Arrays.toString(key), new HashSet<>());
                    // 添加column索引
                    columnOffsetSet.add(fileOffset);
                    columnPosMap.put(Arrays.toString(key), columnOffsetSet);

                    int keyLen = key.length;
                    int keyOffset = keyLen + vinOffset;
                    int valueLen = value.length;
                    int valueOffset = valueLen + keyOffset;

                    ByteBuffer buffer = ByteBuffer.allocate(28 + vinLen + keyLen + it.getValueType().length + valueLen + 1000);
                    buffer.putInt(it.getCrc());
                    buffer.putInt(fileOffset);
                    buffer.putLong(it.getTs());
                    buffer.putInt(vinLen);
                    buffer.putInt(keyLen);
                    buffer.putInt(valueLen);
                    buffer.put(vin.getBytes(StandardCharsets.UTF_8));
                    buffer.put(key);
                    buffer.put(it.getValueType());
                    buffer.put(value);
//                    System.out.println("decoder buffer: "+decoder(buffer));
                    buffer.flip();
                    flushBuf.put(buffer);
                    fileOffset = valueOffset;
            });
            flushBuf.flip();
            System.out.println("flushBuf: " +Arrays.toString(flushBuf.array()));

//            fileChannel.write(flushBuf, fileOffset);
            fileChannel.write(flushBuf, fileOffset, flushBuf, new CompletionHandler<Integer, ByteBuffer>() {
                @Override
                public void completed(Integer result, ByteBuffer attachment) {
                    if (queue.peek() != null) {
                        flushFromQueue();
                    }
                }

                @Override
                public void failed(Throwable exc, ByteBuffer attachment) {
                    // 写入失败
                }
            });
        } catch (IOException e) {
            // todo 日志处理
            e.printStackTrace();
        }
    }

    public LinkedBlockingQueue<List<RealWriteReq>> getQueue() {
        return queue;
    }

    public void flush() {
        flushFromQueue();
    }

    private String decoder(ByteBuffer buffer) {
        try {
            Charset charset = StandardCharsets.UTF_8;
            CharsetDecoder decoder = charset.newDecoder();
            buffer.flip();
            CharBuffer buffer1 = decoder.decode(buffer);
            return buffer1.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "xx";
    }

    //    private int writeRequestLen(WriteRequest request) {
    //        int len = request.getTableName().getBytes().length;
    //    }
}
