package com.unimas.kstream;

import org.junit.Test;

import java.time.ZoneOffset;


/**
 * Created by ethan
 * 17-11-24
 */
public class KsUtilsTest {
    @Test
    public void dateTimeFormatter() throws Exception {
    }

    @Test
    public void nonNullAndEmpty() throws Exception {
    }

    @Test
    public void transDate() throws Exception {
        String value = "2017-11-24 15:20:35.000Z"; //1511536835000
        value = "2017-11-24 15:20:35.000";         //1511508035000
        value = "2017-11-24 15:20:35";             //1511508035000
        value = "2017-11-24";                      //1511481600000
        String format = "uuuu-MM-dd HH:mm:ss.SSSX";
        format = "uuuu-MM-dd HH:mm:ss.SSS";
        format = "uuuu-MM-dd HH:mm:ss";
        format = "uuuu-MM-dd";
        System.out.println(KsUtils.transDate(
                value,
                KsUtils.dateTimeFormatter(format, null),
                ZoneOffset.of("+08:00")));
    }

    @Test
    public void transDate1() throws Exception {
    }

    @Test
    public void transDate2() throws Exception {
    }

}