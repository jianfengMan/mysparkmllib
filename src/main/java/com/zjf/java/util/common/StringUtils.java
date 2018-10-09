package com.zjf.java.util.common;

import org.apache.log4j.Logger;

import java.net.URLEncoder;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Description: String工具类
 * Author: zhangjianfeng
 */
public class StringUtils extends org.apache.commons.lang.StringUtils {
    @SuppressWarnings("unused")
    private static Logger log = Logger.getLogger(StringUtils.class);
    private static Random randGen = null;
    private static char[] numbersAndLetters = null;

    /**
     * 首字母转小写
     */
    public static String toLowerCaseFirstOne(String s) {
        if (Character.isLowerCase(s.charAt(0))) {
            return s;
        } else {
            return (new StringBuilder()).append(Character.toLowerCase(s.charAt(0))).append(s.substring(1)).toString();
        }
    }

    /**
     * 首字母转大写
     */
    public static String toUpperCaseFirstOne(String s) {
        if (Character.isUpperCase(s.charAt(0))) {
            return s;
        } else {
            return (new StringBuilder()).append(Character.toUpperCase(s.charAt(0))).append(s.substring(1)).toString();
        }
    }

    /**
     * 生成随机码
     */
    public static final String randomUUID() {
        return UUID.randomUUID().toString();
    }

    /**
     * 生成随机码，去掉-，共32位
     */
    public static final String randomUUIDSplit() {
        return randomUUID().replaceAll("-", "");
    }


    /**
     * 将驼峰风格替换为下划线风格
     */
    public static String camelhumpToUnderline(String str) {
        Matcher matcher = Pattern.compile("[A-Z]").matcher(str);
        StringBuilder builder = new StringBuilder(str);
        for (int i = 0; matcher.find(); i++) {
            builder.replace(matcher.start() + i, matcher.end() + i, "_" + matcher.group().toLowerCase());
        }
        if (builder.charAt(0) == '_') {
            builder.deleteCharAt(0);
        }
        return builder.toString();
    }

    /**
     * 将下划线风格替换为驼峰风格
     */
    public static String underlineToCamelhump(String str) {
        Matcher matcher = Pattern.compile("_[A-Z]").matcher(str);
        StringBuilder builder = new StringBuilder(str);
        for (int i = 0; matcher.find(); i++) {
            builder.replace(matcher.start() - i, matcher.end() - i, matcher.group().substring(1).toUpperCase());
        }
        if (Character.isUpperCase(builder.charAt(0))) {
            builder.replace(0, 1, String.valueOf(Character.toLowerCase(builder.charAt(0))));
        }
        return builder.toString();
    }

    /**
     * 模糊字符串
     *
     * @param str      处理字符串
     * @param startLen 左边长度
     * @param endLen   右边未脱敏长度
     * @param vagueLen 脱敏字符的长度
     * @param vague    脱敏字符
     * @return
     */
    public static String getVague(String str, int startLen, int endLen, int vagueLen, String vague) {
        if (!isNotNull(str) || str.length() < (startLen + endLen)) {
            return str;
        }
        StringBuffer sb = new StringBuffer();
        sb.append(str.substring(0, startLen));
        for (int i = 0; i < vagueLen; i++) {
            sb.append(vague);
        }
        sb.append(str.substring(str.length() - endLen));
        return sb.toString();
    }


    /**
     * @Description: 产生随机字符串
     */
    public static final String randomNumber(int length) {
        if (length < 1) {
            return null;
        }
        if (randGen == null) {
            randGen = new Random();
            numbersAndLetters = ("0123456789").toCharArray();
        }
        char[] randBuffer = new char[length];
        for (int i = 0; i < randBuffer.length; i++) {
            randBuffer[i] = numbersAndLetters[randGen.nextInt(9)];
        }
        return new String(randBuffer);
    }

    public static boolean isNotNull(String str) {
        return !isNull(str);
    }

    public static boolean isNull(String str) {
        if (StringUtils.isBlank(str) || str.equals("null")) {
            return true;
        }
        return false;
    }

    /**
     * 移除字符串中的特殊字符 / \ + =
     */
    public static String getReplaceStr(String str) {
        if (isNotEmpty(str)) {
            return str.replaceAll("/", "").replaceAll("\\\\", "").replaceAll("\\+", "").replaceAll("=", "");
        }
        return str;
    }

    /**
     * 将HashMap参数组装成字符串
     */
    public static String parseParams(Map<String, String[]> map) {
        StringBuffer sb = new StringBuffer();
        if (map != null && map.keySet().size() > 0) {
            for (Entry<String, String[]> e : map.entrySet()) {
                sb.append(e.getKey());
                sb.append("=");
                if (StringUtils.isEmpty(e.getValue()[0]) || "null".equals(e.getValue()[0])) {
                    sb.append("");
                } else {
                    sb.append(URLEncoder.encode(e.getValue()[0]));
                }
                sb.append("&");
            }
            return sb.substring(0, sb.length() - 1);
        }
        return sb.toString();
    }
}
