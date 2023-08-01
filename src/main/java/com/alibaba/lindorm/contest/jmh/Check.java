package com.alibaba.lindorm.contest.jmh;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-01
 */
public class Check<T> {

    public void check(PromoEnum promoEnum, T value) {
        promoEnum.check(value);
    }
}
