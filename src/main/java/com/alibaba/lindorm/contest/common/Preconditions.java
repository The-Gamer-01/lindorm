package com.alibaba.lindorm.contest.common;

import java.io.IOException;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-07-31
 */
public final class Preconditions {

    public static void checkFileState(boolean b, String msg) throws IOException {
        if (!b) {
            throw new IOException(msg);
        }
    }
}
