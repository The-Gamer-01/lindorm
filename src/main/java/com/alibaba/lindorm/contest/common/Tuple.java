package com.alibaba.lindorm.contest.common;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-04
 */
public class Tuple {

    private Tuple() {
        throw new UnsupportedOperationException();
    }

    public static <A, B> TwoTuple<A, B> tuple(final A a, final B b) {
        return new TwoTuple<>(a, b);
    }
}
