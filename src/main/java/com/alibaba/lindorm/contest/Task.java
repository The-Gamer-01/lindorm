package com.alibaba.lindorm.contest;

import static com.alibaba.lindorm.contest.common.MoreRunnables.runOnce;
import static com.alibaba.lindorm.contest.common.MoreRunnables.runOnceSilently;
import static com.alibaba.lindorm.contest.common.MoreRunnables.runOnceSilentlyFailSafe;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-01
 */
public class Task {

    private static final int THREAD_POOL_SIZE = 1;
    // 使用可用处理器数量作为线程池大小
    private static final int PERIOD = 10; // 执行周期为30秒

    private static final ScheduledThreadPoolExecutor EXECUTOR = new ScheduledThreadPoolExecutor(THREAD_POOL_SIZE);

    public void execute(Runnable runnable) {
        runOnceSilentlyFailSafe(
                () -> EXECUTOR.scheduleAtFixedRate(runnable, 0, PERIOD, TimeUnit.SECONDS),
                () -> {throw new RuntimeException("TASK EXECUTOR FAILD");});
    }

    public void shutdown() {
        EXECUTOR.shutdownNow();
    }
}
