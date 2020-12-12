package it.kenn.util;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalUnit;

/**
 * 时间工具类
 *
 * @author yulei
 * 2020年11月25日23点10分
 */
public final class DateUtils {

    public static Long parseStringToLong(String time, DateTimeFormatter pattern, int offset, TemporalUnit unit) {
//        DateTimeFormatter pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        LocalDateTime dateTime = null;
        if (offset > 0){
            dateTime = LocalDateTime.parse(time, pattern).plus(offset, unit);
        }else if (offset < 0){
            dateTime = LocalDateTime.parse(time, pattern).minus(Math.abs(offset), unit);
        }else {
            dateTime = LocalDateTime.parse(time, pattern);
        }
        return dateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    public static Long parseStringToLong(String time, DateTimeFormatter pattern) {
        return parseStringToLong(time, pattern, 0, null);
    }

    public static Long parseStringToLong(String time) {
        return parseStringToLong(time, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
    }

    public static LocalDateTime parseStringToDateTime(String time, DateTimeFormatter pattern) {
        return LocalDateTime.parse(time, pattern);
    }

    public static LocalDateTime parseStringToDateTime(String time) {
        return parseStringToDateTime(time, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
    }
}
