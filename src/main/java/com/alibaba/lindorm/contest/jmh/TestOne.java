package com.alibaba.lindorm.contest.jmh;

import java.io.File;

import com.alibaba.lindorm.contest.TSDBEngineImpl;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-07-31
 */
public class TestOne {

    public static void main(String[] args) throws Exception {
        TSDBEngineImpl engine = new TSDBEngineImpl(new File("temp"));
        engine.connect();
        engine.createTable("azh", null);
    }
}
