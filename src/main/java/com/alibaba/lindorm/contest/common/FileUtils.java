package com.alibaba.lindorm.contest.common;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-05
 */
public class FileUtils {

    public static List<File> traverseFolder(File folder) {
        List<File> fileList = new ArrayList<>();
        if (folder.isDirectory()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        // 如果是子文件夹，递归遍历
                        traverseFolder(file);
                    } else {
                        // 处理文件
                        fileList.add(file);
                    }
                }
            }
        }
        return fileList;
    }
}
