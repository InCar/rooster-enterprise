package com.incarcloud.rooster.util;

import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;

/**
 * HBase工具类
 *
 * @author Aaric, created on 2017-12-11T17:15.
 * @since 1.0-SNAPSHOT
 */
public final class HBaseUtil {

    /**
     * 默认字符串UTF-8
     */
    public static final String DEFAULT_CHARSET_UTF8 = "UTF-8";

    /**
     * 获得字符串的字节数组
     *
     * @param string 字符串
     * @return
     * @throws UnsupportedEncodingException
     */
    public static byte[] valueOf(String string) throws UnsupportedEncodingException {
        if (StringUtils.isBlank(string)) {
            throw new IllegalArgumentException("string is blank.");
        }
        return string.getBytes(DEFAULT_CHARSET_UTF8);
    }
}
