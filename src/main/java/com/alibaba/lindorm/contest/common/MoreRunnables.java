package com.alibaba.lindorm.contest.common;

import static com.alibaba.lindorm.contest.common.MoreSupplizers.lazy;

import java.util.function.Supplier;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-07-31
 */
public class MoreRunnables {


    public static Runnable runOnce(Runnable runnable) {
        return new Runnable() {
            private final Supplier<Void> supplier = lazy(() -> {
                runnable.run();
                return null;
            });

            @Override
            public void run() {
                supplier.get();
            }
        };
    }

    public static void runOnceSilently(Runnable runnable) {
        new Runnable() {
            private final Supplier<Void> supplier = lazy(() -> {
                runnable.run();
                return null;
            });

            @Override
            public void run() {
                supplier.get();
            }
        };
    }

    public static <R> void runOnceSilentlyFailSafe(Supplier<R> run, Supplier<R> runWithFailedGotSafe) {
        try {
            run.get();
        } catch (Exception e) {
            runWithFailedGotSafe.get();
        }
    }

    public static <R> R supplyWithFailSafe(Supplier<R> run, Supplier<R> runWithFailedGotSafe) {
        try {
            return run.get();
        } catch (Exception e) {
            return runWithFailedGotSafe.get();
        }
    }
}
