package com.alibaba.lindorm.contest.common;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-07-31
 */
public class MoreSupplizers<T> implements Supplier<T>  {

    private final Supplier<? extends T> supplier;

    private T value;

    private MoreSupplizers(Supplier<? extends T> supplier) {
        this.supplier = supplier;
    }

    @Override
    public T get() {
        if (value == null) {
            synchronized (this) {
                T newValue = supplier.get();
                if (newValue == null) {
                    throw new RuntimeException("lazy value not be null");
                }
                value = newValue;
            }
        }
        return value;
    }

    public static <T> MoreSupplizers<T> lazy(Supplier<? extends T> supplier) {
        return new MoreSupplizers<>(supplier);
    }
}
