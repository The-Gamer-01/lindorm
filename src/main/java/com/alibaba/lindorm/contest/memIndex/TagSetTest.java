package com.alibaba.lindorm.contest.memIndex;

import org.junit.Test;

import static org.junit.Assert.*;

public class TagSetTest {

    @Test
    public void formTagKey() {
        Tag[] tags = new Tag[3];
        tags[0] = new Tag("1K".getBytes(), "1V".getBytes());
        tags[1] = new Tag("2K".getBytes(), "2V".getBytes());
        tags[2] = new Tag("3K".getBytes(), "3V".getBytes());
        String tagKey = TagSet.formTagKey(tags);
        assert tagKey.equals("1K|2K|3K|1V|2V|3V");
    }
}