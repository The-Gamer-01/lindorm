package com.alibaba.lindorm.contest.jmh;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-01
 */
public enum PromoEnum {

    PROMO_PROVIDER("promoProvider") {
        public <T> void check(T t) {

        }
    };

    public abstract <T> void check(T t);

    private String value;

    PromoEnum(String value) {
        this.value = value;
    }


    public static PromoEnum fromValue(String value) {
        return Arrays.stream(PromoEnum.values())
                .filter(it -> StringUtils.equals(value, it.value)).findAny()
                .orElseThrow();
    }

    public void test() {
        Map<String, String> promoKconf = new HashMap<>();
        promoKconf.forEach((k, v) -> {
            PromoEnum.fromValue(k).check(v);
        });
    }
}
