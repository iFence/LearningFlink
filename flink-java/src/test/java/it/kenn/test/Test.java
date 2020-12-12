package it.kenn.test;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class Test {
    public static void main(String[] args) {
        String stime =  "2020-11-18T04:31:40.886Z";
        DateTimeFormatter pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        LocalDateTime parse = LocalDateTime.parse(stime, pattern);
        long ts = parse.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        System.out.println(parse+" \nts:"+ts);
    }
}
