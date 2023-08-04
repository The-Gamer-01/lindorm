package com.alibaba.lindorm.contest.jmh;

import static com.alibaba.lindorm.contest.common.NumberUtils.bytesToDouble;
import static com.alibaba.lindorm.contest.common.NumberUtils.bytesToInt;
import static com.alibaba.lindorm.contest.common.NumberUtils.doubleToBytes;
import static com.alibaba.lindorm.contest.common.NumberUtils.intToBytes;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhaozhenhang <zhaozhenhang@kuaishou.com>
 * Created on 2023-08-04
 */
public class CompressTest {

    public static String compress(String input) {
        Map<String, Integer> dictionary = new HashMap<>(); // 字典
        StringBuilder compressed = new StringBuilder(); // 压缩结果

        int index = 0; // 字典索引
        String currentChar = String.valueOf(input.charAt(0)); // 当前字符

        for (int i = 1; i < input.length(); i++) {
            String nextChar = String.valueOf(input.charAt(i));

            if (dictionary.containsKey(currentChar + nextChar)) {
                currentChar += nextChar;
            } else {
                compressed.append(dictionary.getOrDefault(currentChar, -1)).append(" ");
                dictionary.put(currentChar + nextChar, index);
                index++;
                currentChar = nextChar;
            }
        }

        compressed.append(dictionary.getOrDefault(currentChar, -1));

        return compressed.toString();
    }

    public static String decompress(String input) {
        Map<Integer, String> dictionary = new HashMap<>(); // 字典
        StringBuilder decompressed = new StringBuilder(); // 解压结果

        int index = 0; // 字典索引

        for (int i = 0; i < input.length(); i++) {
            StringBuilder sequence = new StringBuilder(String.valueOf(input.charAt(i)));

            while (i + 1 < input.length() && Character.isDigit(input.charAt(i + 1))) {
                sequence.append(input.charAt(i + 1));
                i++;
            }

            System.out.println(sequence.toString());
            int dictIndex = Integer.parseInt(sequence.toString());

            if (!dictionary.containsKey(dictIndex)) {
                String entry = dictionary.get(index - 1) + dictionary.get(index - 1).charAt(0);
                dictionary.put(index, entry);
                decompressed.append(entry);
                index++;
            } else {
                decompressed.append(dictionary.get(dictIndex));
            }
        }

        return decompressed.toString();
    }

    public static void main(String[] args) {
//        String input = "abababababab";
//        String compressed = compress(input);
//        System.out.println("Compressed: " + compressed);
//
//        String decompressed = decompress(compressed);
//        System.out.println("Decompressed: " + decompressed);

        byte[] num = doubleToBytes(19232.232);
        System.out.println(Arrays.toString(num));
        double num2 = bytesToDouble(num);
        System.out.println(num2);
    }
}
