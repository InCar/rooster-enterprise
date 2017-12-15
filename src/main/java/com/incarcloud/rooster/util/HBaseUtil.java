package com.incarcloud.rooster.util;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase工具类
 *
 * @author Aaric, created on 2017-12-11T17:15.
 * @since 1.0-SNAPSHOT
 */
public final class HBaseUtil {

    /**
     * String转BinaryComparator对象
     *
     * @param string 字符串
     * @return
     */
    public static BinaryComparator toBinaryComparator(String string) {
        return new BinaryComparator(Bytes.toBytes(string));
    }
}
