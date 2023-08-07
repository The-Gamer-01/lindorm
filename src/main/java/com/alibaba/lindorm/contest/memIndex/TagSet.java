package com.alibaba.lindorm.contest.memIndex;

import java.util.Arrays;

public class TagSet implements Comparable<TagSet> {
    private Tag[] tags;

    private String tagKey;

    public TagSet(Tag[] _tags) {
        this.tags = _tags;
        Arrays.sort(tags);
        tagKey = formTagKey(tags);
    }


    public static String formTagKey(Tag[] tags) {
        int size = 0;
        for(int i = 0; i < tags.length; i++) {
            size += tags[i].key.length + tags[i].value.length;
        }
        size += tags.length*2-1;
        byte[] buf = new byte[size];
        int curSize = 0;
        for (int i = 0; i < tags.length; i++) {
            System.arraycopy(tags[i].key, 0, buf, curSize, tags[i].key.length);
            buf[curSize+tags[i].key.length] = '|';
            curSize += tags[i].key.length+1;
        }

        for (int i = 0; i < tags.length; i++) {
            System.arraycopy(tags[i].value, 0, buf, curSize, tags[i].value.length);
            if (i < tags.length-1) {
                buf[curSize+tags[i].value.length] = '|';
                curSize += tags[i].value.length+1;
            } else {
                curSize += tags[i].value.length;
            }
        }
        return new String(buf);
    }

    @Override
    public int compareTo(TagSet o) {
        return this.tagKey.compareTo(o.tagKey);
    }
}
