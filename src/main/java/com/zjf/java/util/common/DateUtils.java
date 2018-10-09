package com.zjf.java.util.common;

import java.text.Format;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Description:日期工具类
 * Author: zhangjianfeng
 */
public class DateUtils {

    private static Calendar CALENDAR = Calendar.getInstance();

    private static final ThreadLocal<SimpleDateFormat> DATE_FORMATER = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat();
        }
    };


    /**
     * 返回当前日期 yyyyMMdd格式
     */
    public static String getCurrDt() {
        return new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance()
                .getTime());
    }


    /**
     * 获取当前年份
     */
    public static Integer getCurrYear() {
        Calendar a = Calendar.getInstance();
        return a.get(Calendar.YEAR);//得到年

    }

    /*
     * 获取当前月份
     */
    public static String getCurrMonth() {
        return new SimpleDateFormat("MM").format(Calendar.getInstance()
                .getTime());
    }

    /**
     * 返回当前日期 yyyyMMdd格式
     *
     * @return
     */
    public static String getFomartDt(String str) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sf = new SimpleDateFormat("yyyyMMdd");
        String rs = "";
        try {
            rs = sf.format(sdf.parse(str));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return rs;
    }


    /**
     * 返回当前日期 yyyy-MM-dd格式
     *
     * @return
     */
    public static String getDtFomart(String str) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sf = new SimpleDateFormat("yyyyMMdd");
        String rs = "";
        try {
            rs = sdf.format(sf.parse(str));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return rs;
    }

    /**
     * 返回当前日期 yyyy-MM-dd格式
     *
     * @return
     */
    public static String getDtTmFomart(String str) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat sf = new SimpleDateFormat("yyyyMMddHHmmss");
        String rs = "";
        try {
            rs = sdf.format(sf.parse(str));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return rs;
    }

    /**
     * 获取当前日期的前N个月或者后N个月
     *
     * @param currCalendar 当前日期
     * @param monthNum     月数
     * @return
     */
    public static Calendar getBeforeOrAfterMonth(Calendar currCalendar, int monthNum) {
        Calendar calendarDt = Calendar.getInstance();
        calendarDt.setTime(currCalendar.getTime());
        calendarDt.add(Calendar.MONTH, monthNum);
        return calendarDt;
    }

    /**
     * 返回当前时间HHmmss格式
     *
     * @return
     */
    public static String getCurrTm() {
        return new SimpleDateFormat("HHmmss").format(Calendar.getInstance()
                .getTime());
    }

    /**
     * 返回当前时间yyyyMMddHHmmssS格式
     *
     * @return
     */
    public static String getCurrDtTm() {
        return new SimpleDateFormat("yyyyMMddHHmmssS").format(Calendar
                .getInstance().getTime());
    }

    /**
     * 返回当前时间yyyyMMddHHmmss格式
     *
     * @return
     */
    public static String getCurDtTm() {
        String rs = "";
        SimpleDateFormat sf = new SimpleDateFormat("yyyyMMddHHmmss");
        rs = sf.format(new Date());
        return rs;
    }

    /**
     * 返回当前时间yyMMddHHmmss格式
     *
     * @return
     */
    public static String getCurYdsTm() {
        String rs = "";
        SimpleDateFormat sf = new SimpleDateFormat("yyMMddHHmmss");
        rs = sf.format(new Date());
        return rs;
    }


    /**
     * 返回当前时间yyyyMMddHHmm格式
     *
     * @return
     */
    public static String getCurYdmTm() {
        String rs = "";
        SimpleDateFormat sf = new SimpleDateFormat("yyyyMMddHHmm");
        rs = sf.format(new Date());
        return rs;
    }

    /**
     * 返回当前时间yyyyMMdd格式
     *
     * @return
     */
    public static String getCurYmdTm() {
        String rs = "";
        SimpleDateFormat sf = new SimpleDateFormat("yyyyMMdd");
        rs = sf.format(new Date());
        return rs;
    }


    /**
     * 返回当前时间yyyyMM格式
     *
     * @return
     */
    public static String getCurYm() {
        String rs = "";
        SimpleDateFormat sf = new SimpleDateFormat("yyyyMM");
        rs = sf.format(new Date());
        return rs;
    }

    /**
     * 返回当前时间HHmmss格式
     *
     * @return
     */
    public static String getCurHmsTm() {
        String rs = "";
        SimpleDateFormat sf = new SimpleDateFormat("HHmmss");
        rs = sf.format(new Date());
        return rs;
    }

    /**
     * 验证是否是日期格式
     *
     * @return
     */
    public static boolean isDateFormat(String date, String format) {
        try {
            Format f = new SimpleDateFormat(format);
            Date d = (Date) f.parseObject(date);
            String tmp = f.format(d);
            return tmp.equals(date);
        } catch (ParseException e) {
            return false;
        }
    }

    /**
     * 获取当天日期的前几天
     *
     * @return
     */
    public static String getBeforeOrAfterDt(int dayNum) {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        calendar.add(Calendar.DAY_OF_YEAR, dayNum);
        Date date = calendar.getTime();
        return sdf.format(date);
    }


    /**
     * 获取当天日期的前几天
     *
     * @return
     */
    public static String getBeforeOrAfterFormatDt(int dayNum) {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        calendar.add(Calendar.DAY_OF_YEAR, dayNum);
        Date date = calendar.getTime();
        return sdf.format(date);
    }

    public static Date stringToDate(String strValue) {
        if (strValue == null || strValue.trim().length() <= 0) {
            return null;
        }
        SimpleDateFormat clsFormat = null;
        if (strValue.length() > 19)
            strValue = strValue.substring(0, 19);
        if (strValue.length() == 19)
            clsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        else if (strValue.length() == 10)
            clsFormat = new SimpleDateFormat("yyyy-MM-dd");
        else if (strValue.length() == 14)
            clsFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        else if (strValue.length() == 8)
            clsFormat = new SimpleDateFormat("yyyyMMdd");

        ParsePosition pos = new ParsePosition(0);
        return clsFormat.parse(strValue, pos);
    }

    /**
     * 取得当前时间
     *
     * @return 当前日期（Date）
     */
    public static Date getCurrentDate() {
        return new Date();
    }

    /**
     * 取得昨天此时的时间
     *
     * @return 昨天日期（Date）
     */
    public static Date getYesterdayDate() {
        return new Date(getCurTimeMillis() - 0x5265c00L);
    }

    /**
     * 取得去过i天的时间
     *
     * @param i 过去时间天数
     * @return 昨天日期（Date）
     */
    public static Date getPastdayDate(int i) {
        return new Date(getCurTimeMillis() - 0x5265c00L * i);
    }

    /**
     * 取得当前时间的长整型表示
     *
     * @return 当前时间（long）
     */
    public static long getCurTimeMillis() {
        return System.currentTimeMillis();
    }

    /**
     * 获取后n天
     *
     * @param date
     * @return Date
     */
    public static Date getNextNDate(Date date, int n) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int day = calendar.get(Calendar.DATE);
        calendar.set(Calendar.DATE, day + n);
        return calendar.getTime();
    }

    /**
     * 获取前n天
     *
     * @param date
     * @return Date
     */
    public static Date getPreviousNDate(Date date, int n) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int day = calendar.get(Calendar.DATE);
        calendar.set(Calendar.DATE, day - n);
        return calendar.getTime();
    }

    /**
     * 获得秒单位的时间戳
     *
     * @return
     */
    public static long getCurTimeSencond() {
        return getCurTimeMillis() / 1000;
    }

    /**
     * 取得当前时间的特定表示格式的字符串
     *
     * @param formatDate 时间格式（如：yyyy/MM/dd hh:mm:ss）
     * @return 当前时间
     */
    public static String getCurFormatDate(String formatDate) {
        Date date = getCurrentDate();
        DATE_FORMATER.get().applyPattern(formatDate);
        return DATE_FORMATER.get().format(date);
    }

    /**
     * 取得某日期时间的特定表示格式的字符串
     *
     * @param format 时间格式
     * @param date   某日期（Date）
     * @return 某日期的字符串
     */
    public static String getDate2Str(String format, Date date) {
        DATE_FORMATER.get().applyPattern(format);
        return DATE_FORMATER.get().format(date);
    }

    /**
     * 将日期转换为长字符串（包含：年-月-日 时:分:秒）
     *
     * @param date 日期
     * @return 返回型如：yyyy-MM-dd HH:mm:ss 的字符串
     */
    public static String getDate2LStr(Date date) {
        return getDate2Str("yyyy-MM-dd HH:mm:ss", date);
    }

    /**
     * 将日期转换为长字符串（包含：年/月/日 时:分:秒）
     *
     * @param date 日期
     * @return 返回型如：yyyy/MM/dd HH:mm:ss 的字符串
     */
    public static String getDate2LStr2(Date date) {
        return getDate2Str("yyyy/MM/dd HH:mm:ss", date);
    }

    /**
     * 将日期转换为中长字符串（包含：年-月-日 时:分）
     *
     * @param date 日期
     * @return 返回型如：yyyy-MM-dd HH:mm 的字符串
     */
    public static String getDate2MStr(Date date) {
        return getDate2Str("yyyy-MM-dd HH:mm", date);
    }

    /**
     * 将日期转换为中长字符串（包含：年/月/日 时:分）
     *
     * @param date 日期
     * @return 返回型如：yyyy/MM/dd HH:mm 的字符串
     */
    public static String getDate2MStr2(Date date) {
        return getDate2Str("yyyy/MM/dd HH:mm", date);
    }

    /**
     * 将日期转换为短字符串（包含：年-月-日）
     *
     * @param date 日期
     * @return 返回型如：yyyy-MM-dd 的字符串
     */
    public static String getDate2SStr(Date date) {
        return getDate2Str("yyyy-MM-dd", date);
    }

    /**
     * 将日期转换为短字符串（包含：年/月/日）
     *
     * @param date 日期
     * @return 返回型如：yyyy/MM/dd 的字符串
     */
    public static String getDate2SStr2(Date date) {
        return getDate2Str("yyyy/MM/dd", date);
    }

    /**
     * 取得型如：yyyyMMddhhmmss的字符串
     *
     * @param date
     * @return 返回型如：yyyyMMddhhmmss 的字符串
     */
    public static String getDate2All(Date date) {
        return getDate2Str("yyyyMMddhhmmss", date);
    }

    /**
     * 将长整型数据转换为Date后，再转换为型如yyyy-MM-dd HH:mm:ss的长字符串
     *
     * @param l 表示某日期的长整型数据
     * @return 日期型的字符串
     */
    public static String getLong2LStr(long l) {
        Date date = getLongToDate(l);
        return getDate2LStr(date);
    }

    /**
     * 将长整型数据转换为Date后，再转换为型如yyyy-MM-dd的长字符串
     *
     * @param l 表示某日期的长整型数据
     * @return 日期型的字符串
     */
    public static String getLong2SStr(long l) {
        Date date = getLongToDate(l);
        return getDate2SStr(date);
    }

    /**
     * 将长整型数据转换为Date后，再转换指定格式的字符串
     *
     * @param l          表示某日期的长整型数据
     * @param formatDate 指定的日期格式
     * @return 日期型的字符串
     */
    public static String getLong2SStr(long l, String formatDate) {
        Date date = getLongToDate(l);
        DATE_FORMATER.get().applyPattern(formatDate);
        return DATE_FORMATER.get().format(date);
    }

    public static Date getStrToDate(String format, String str) {
        DATE_FORMATER.get().applyPattern(format);
        try {
            return DATE_FORMATER.get().parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 将某指定的字符串转换为某类型的字符串
     *
     * @param format 转换格式
     * @param str    需要转换的字符串
     * @return 转换后的字符串
     */
    public static String getStr2Str(String format, String str) {
        Date date = getStrToDate(format, str);
        return getDate2Str(format, date);
    }

    /**
     * 将某指定的字符串转换为型如：yyyy-MM-dd HH:mm:ss的时间
     *
     * @param str 将被转换为Date的字符串
     * @return 转换后的Date
     */
    public static Date getStr2LDate(String str) {
        return getStrToDate("yyyy-MM-dd HH:mm:ss", str);
    }

    /**
     * 将某指定的字符串转换为型如：yyyy/MM/dd HH:mm:ss的时间
     *
     * @param str 将被转换为Date的字符串
     * @return 转换后的Date
     */
    public static Date getStr2LDate2(String str) {
        return getStrToDate("yyyy/MM/dd HH:mm:ss", str);
    }

    /**
     * 将某指定的字符串转换为型如：yyyy-MM-dd HH:mm的时间
     *
     * @param str 将被转换为Date的字符串
     * @return 转换后的Date
     */
    public static Date getStr2MDate(String str) {
        return getStrToDate("yyyy-MM-dd HH:mm", str);
    }

    /**
     * 将某指定的字符串转换为型如：yyyy/MM/dd HH:mm的时间
     *
     * @param str 将被转换为Date的字符串
     * @return 转换后的Date
     */
    public static Date getStr2MDate2(String str) {
        return getStrToDate("yyyy/MM/dd HH:mm", str);
    }

    /**
     * 将某指定的字符串转换为型如：yyyy-MM-dd的时间
     *
     * @param str 将被转换为Date的字符串
     * @return 转换后的Date
     */
    public static Date getStr2SDate(String str) {
        return getStrToDate("yyyy-MM-dd", str);
    }

    /**
     * 将某指定的字符串转换为型如：yyyy-MM-dd的时间
     *
     * @param str 将被转换为Date的字符串
     * @return 转换后的Date
     */
    public static Date getStr2SDate2(String str) {
        return getStrToDate("yyyy/MM/dd", str);
    }

    /**
     * 将某指定的字符串转换为型如：yyyy-MM-dd的时间
     *
     * @param str 将被转换为Date的字符串
     * @return 转换后的Date
     */
    public static Date getStr2SDate3(String str) {
        return getStrToDate("yyyyMMdd", str);
    }

    /**
     * 将某长整型数据转换为日期
     *
     * @param l 长整型数据
     * @return 转换后的日期
     */
    public static Date getLongToDate(long l) {
        return new Date(l);
    }

    /**
     * 以分钟的形式表示某长整型数据表示的时间到当前时间的间隔
     *
     * @param l 长整型数据
     * @return 相隔的分钟数
     */
    public static int getOffMinutes(long l) {
        return getOffMinutes(l, getCurTimeMillis());
    }

    /**
     * 以分钟的形式表示两个长整型数表示的时间间隔
     *
     * @param from 开始的长整型数据
     * @param to   结束的长整型数据
     * @return 相隔的分钟数
     */
    public static int getOffMinutes(long from, long to) {
        return (int) ((to - from) / 60000L);
    }

    /**
     * 以微秒的形式赋值给一个Calendar实例
     *
     * @param l 用来表示时间的长整型数据
     */
    public static void setCalendar(long l) {
        CALENDAR.clear();
        CALENDAR.setTimeInMillis(l);
    }

    /**
     * 以日期的形式赋值给某Calendar
     *
     * @param date 指定日期
     */
    public static void setCalendar(Date date) {
        CALENDAR.clear();
        CALENDAR.setTime(date);
    }

    /**
     * 在此之前要由一个Calendar实例的存在
     *
     * @return 返回某年
     */
    public static int getYear() {
        return CALENDAR.get(1);
    }

    /**
     * 在此之前要由一个Calendar实例的存在
     *
     * @return 返回某月
     */
    public static int getMonth() {
        return CALENDAR.get(2) + 1;
    }

    /**
     * 在此之前要由一个Calendar实例的存在
     *
     * @return 返回某天
     */
    public static int getDay() {
        return CALENDAR.get(5);
    }

    /**
     * 在此之前要由一个Calendar实例的存在
     *
     * @return 返回某小时
     */
    public static int getHour() {
        return CALENDAR.get(11);
    }

    /**
     * 在此之前要由一个Calendar实例的存在
     *
     * @return 返回某分钟
     */
    public static int getMinute() {
        return CALENDAR.get(12);
    }

    /**
     * 在此之前要由一个Calendar实例的存在
     *
     * @return 返回某秒
     */
    public static int getSecond() {
        return CALENDAR.get(13);
    }


    /**
     * 获取本月第一天的秒值
     *
     * @return
     */
    public static String getCurrMonthFirstDaySecond() {
        //获取当前月第一天：
        Calendar c = Calendar.getInstance();
        c.add(Calendar.MONTH, 0);
        c.set(Calendar.DAY_OF_MONTH, 1);//设置为1号,当前日期既为本月第一天
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        return "" + c.getTimeInMillis() / 1000;
    }


    /**
     * 获取当月最后一天的秒值
     *
     * @return
     */
    public static String getCurrMonthLastDaySecond() {
        //获取当前月最后一天
        Calendar ca = Calendar.getInstance();
        ca.set(Calendar.DAY_OF_MONTH, ca.getActualMaximum(Calendar.DAY_OF_MONTH));
        ca.set(Calendar.HOUR_OF_DAY, 23);
        ca.set(Calendar.MINUTE, 59);
        ca.set(Calendar.SECOND, 59);
        return "" + ca.getTimeInMillis() / 1000;
    }

    /**
     * 获取本周第一天的秒值
     *
     * @return
     */
    public static String getCurrWeekFirstDaySecond(int firstDayOfWeek) {
        //获取当前月第一天：
        Calendar c = Calendar.getInstance();
        c.setFirstDayOfWeek(firstDayOfWeek);
        c.set(Calendar.DAY_OF_WEEK, firstDayOfWeek);//设置为1号,当前日期既为本月第一天
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        return "" + c.getTimeInMillis() / 1000;
    }


    /**
     * 获取本周最后一天的秒值
     *
     * @return
     */
    public static String getCurrWeekLastDaySecond(int firstDayOfWeek, int lastDayOfWeek) {
        //获取当前月最后一天
        Calendar c = Calendar.getInstance();
        c.setFirstDayOfWeek(firstDayOfWeek);
        c.set(Calendar.DAY_OF_WEEK, lastDayOfWeek);//设置为1号,当前日期既为本月第一天
        c.set(Calendar.HOUR_OF_DAY, 23);
        c.set(Calendar.MINUTE, 59);
        c.set(Calendar.SECOND, 59);
        return "" + c.getTimeInMillis() / 1000;
    }

    /**
     * 获取当天的第一秒
     *
     * @return
     */
    public static long getCurrDayStartSecond() {
        //获取当前月最后一天
        Calendar ca = Calendar.getInstance();
        ca.set(Calendar.HOUR, 0);
        ca.set(Calendar.MINUTE, 0);
        ca.set(Calendar.SECOND, 0);
        return ca.getTimeInMillis() / 1000;
    }

    /**
     * 获取当天的最后一秒
     *
     * @return
     */
    public static long getCurrDayEndSecond() {
        Calendar ca = Calendar.getInstance();
        ca.set(Calendar.HOUR, 23);
        ca.set(Calendar.MINUTE, 59);
        ca.set(Calendar.SECOND, 59);
        return ca.getTimeInMillis() / 1000;
    }

    /**
     * 获取date类型当天的第一秒
     *
     * @return
     */
    public static Date getCurrDayStartDate() {
        Calendar ca = Calendar.getInstance();
        ca.set(Calendar.HOUR_OF_DAY, 0);
        ca.set(Calendar.MINUTE, 0);
        ca.set(Calendar.SECOND, 0);
        return ca.getTime();
    }

    /**
     * 获取date类型当天的最后一秒
     *
     * @return
     */
    public static Date getCurrDayEndDate() {
        Calendar ca = Calendar.getInstance();
        ca.set(Calendar.HOUR_OF_DAY, 23);
        ca.set(Calendar.MINUTE, 59);
        ca.set(Calendar.SECOND, 59);
        return ca.getTime();
    }


    public static String getPhpTime(String dateStr) {

        Date date = getStrToDate("yyyy-MM-dd", dateStr);
        String phpTime = date.getTime() / 1000 + "";
        return phpTime;
    }

    /**
     * 按照unit获取baseTime的时间加上add的结果
     * 如获取当前
     *
     * @param unit
     * @param add
     * @param baseTime
     * @return
     */
    public static long getAddSecondTime(int unit, int add, long baseTime) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(baseTime);
        calendar.set(unit, calendar.get(unit) + add);
        return calendar.getTimeInMillis() / 1000;
    }

    /**
     * 获得所给日期的0点时间的秒数
     *
     * @param date
     * @return
     */
    public static long get0PointSecondTime(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return calendar.getTimeInMillis() / 1000;
    }


    /**
     * 获得所给日期的0点时间的秒数
     *
     * @return
     */
    public static Date secondToDate(String second) {

        return new Date(Long.parseLong(second) * 1000);
    }


    /**
     * 格式化好的时间转成时间戳
     *
     * @param date
     * @return
     * @author zhangteng
     */
    public static long getDateStr2long(String date) {
        Calendar c = Calendar.getInstance();
        try {
            c.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return c.getTimeInMillis();
    }

    /**
     * 格式化好的时间转成时间戳
     *
     * @param date
     * @return
     * @author zhangteng
     */
    public static long getDateStr2long(String format, String date) {
        Calendar c = Calendar.getInstance();
        try {
            c.setTime(new SimpleDateFormat(format).parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return c.getTimeInMillis();
    }

    /**
     * 获取某天的第一秒, 该时间是相对于当前时间来说。
     *
     * @param relativeDay - 相对天数。i.e.:如果获取当天的第一秒该参数为0；获取昨天的第一秒该参数为-1；获取明天的第一秒该参数为1
     * @return
     */
    public static Date getRelativeStartDate(int relativeDay) {
        Calendar ca = Calendar.getInstance();
        ca.add(Calendar.DAY_OF_MONTH, relativeDay);
        ca.set(Calendar.HOUR_OF_DAY, 0);
        ca.set(Calendar.MINUTE, 0);
        ca.set(Calendar.SECOND, 0);
        ca.set(Calendar.MILLISECOND, 0);
        return ca.getTime();
    }

    /**
     * 获取某天的最后一秒, 该时间是相对于当前时间来说。
     *
     * @param relativeDay - 相对天数。i.e.:如果获取当天的最后一秒该参数为0；获取昨天的最后一秒该参数为-1；获取明天的最后一秒该参数为1
     * @return
     */
    public static Date getRelativeEndDate(int relativeDay) {
        Calendar ca = Calendar.getInstance();
        ca.add(Calendar.DAY_OF_MONTH, relativeDay + 1);
        ca.set(Calendar.HOUR_OF_DAY, 0);
        ca.set(Calendar.MINUTE, 0);
        ca.set(Calendar.SECOND, 0);
        ca.set(Calendar.MILLISECOND, 0);
        return ca.getTime();
    }

    /**
     * 通过两个date 来获取两个date之间相差天数
     *
     * @param beginTime 开始时间
     * @param endTime   结束时间
     * @return
     */
    public static int daysBetween(String beginTime, String endTime) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(sdf.parse(beginTime));
        long time1 = cal.getTimeInMillis();
        cal.setTime(sdf.parse(endTime));
        long time2 = cal.getTimeInMillis();
        long between_days = (time2 - time1) / (1000 * 3600 * 24);

        return Integer.parseInt(String.valueOf(between_days));
    }

    /**
     * 计算两个日期之间相差的天数
     *
     * @param smdate 较小的时间
     * @param bdate  较大的时间
     * @return 相差天数
     * @throws ParseException
     */
    public static int daysBetween(Date smdate, Date bdate) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        smdate = sdf.parse(sdf.format(smdate));
        bdate = sdf.parse(sdf.format(bdate));
        Calendar cal = Calendar.getInstance();
        cal.setTime(smdate);
        long time1 = cal.getTimeInMillis();
        cal.setTime(bdate);
        long time2 = cal.getTimeInMillis();
        long between_days = (time2 - time1) / (1000 * 3600 * 24);

        return Integer.parseInt(String.valueOf(between_days));
    }

    /**
     * 比较两个 时间的大小 （d1>=d2 true）
     *
     * @param d1
     * @param d2
     * @return
     */
    public static boolean compareDate(Date d1, Date d2) {
        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();
        c1.setTime(d1);
        c2.setTime(d2);
        int result = c1.compareTo(c2);
        if (result >= 0)
            return true;
        else
            return false;
    }

}
