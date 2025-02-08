package com.quanta.util;

import java.sql.Timestamp;
import java.util.Calendar;

public class Dates {

    public static Timestamp toTimestamp(int yyyy, int mm, int dd) {
        return new Timestamp(getCalendar(yyyy, mm, dd).getTimeInMillis());
    }

    public static Calendar getCalendar(String yyyy, String mm, String dd,
                                       String hh, String mi, String ss) {
        Calendar cal = getCalendar(yyyy, mm, dd);
        cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(hh));
        cal.set(Calendar.MINUTE, Integer.parseInt(mi));
        cal.set(Calendar.SECOND, Integer.parseInt(ss));
        return cal;
    }

    public static Calendar getCalendar(int yyyy, int mm, int dd, int hh,
                                       int mi) {
        return getCalendar(yyyy, mm, dd, hh, mi, 0);
    }
    public static Calendar getCalendar(int yyyy, int mm, int dd, int hh,
                                       int mi, int ss) {
        Calendar cal = getCalendar(yyyy, mm, dd);
        cal.set(Calendar.HOUR_OF_DAY, hh);
        cal.set(Calendar.MINUTE, mi);
        cal.set(Calendar.SECOND, ss);
        return cal;
    }

    public static Calendar getCalendar(String yyyy, String mm, String dd) {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, Integer.parseInt(yyyy));
        cal.set(Calendar.MONTH, Integer.parseInt(mm) - 1);
        cal.set(Calendar.DATE, Integer.parseInt(dd));
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal;
    }

    public static Calendar getCalendar(int yyyy, int mm, int dd) {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, yyyy);
        cal.set(Calendar.MONTH, mm - 1);
        cal.set(Calendar.DATE, dd);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal;
    }
}
