package com.unimas.kstream;

import com.google.common.base.Strings;
import com.unimas.kstream.error.KSConfigNullException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Objects;

/**
 * utils
 */
public class KsUtils {
    /**
     * @param pattern pattern 'uuuu-MM-dd'T'HH:mm:ss.SSSX'
     * @param lang    language 'en','zh'
     * @return dateTimeFormatter {@link DateTimeFormatter}
     */
    public static DateTimeFormatter dateTimeFormatter(String pattern, String lang) {
        Locale locale = new Locale(Objects.toString(lang, KsConfig.DEFAULT_DATETIME_LANG));
        return new DateTimeFormatterBuilder()
                .appendPattern(pattern)
                .toFormatter(locale);
    }

    /**
     * value is not null and empty,else throw {@link KSConfigNullException}
     *
     * @param value   value
     * @param message error message
     * @return value
     * @throws KSConfigNullException exception
     */
    public static String nonNullAndEmpty(String value, String message) throws KSConfigNullException {
        if (Strings.isNullOrEmpty(value)) throw new KSConfigNullException(message);
        return value;
    }

    /**
     * format date value
     *
     * @param value         value
     * @param timeFormatter time formatter {@link DateTimeFormatter}
     * @return temporalAccessor
     */
    public static TemporalAccessor transDate(String value, DateTimeFormatter timeFormatter) {
        TemporalAccessor temporalAccessor;
        try {
            temporalAccessor = ZonedDateTime.parse(value, timeFormatter);
        } catch (DateTimeParseException e1) {
            try {
                temporalAccessor = OffsetDateTime.parse(value, timeFormatter);
            } catch (DateTimeParseException e2) {
                try {
                    temporalAccessor = LocalDateTime.parse(value, timeFormatter);
                } catch (DateTimeParseException e3) {
                    temporalAccessor = LocalDate.parse(value, timeFormatter);
                }
            }
        }
        return temporalAccessor;
    }

    /**
     * format date value
     *
     * @param value         value
     * @param timeFormatter time formatter {@link DateTimeFormatter}
     * @param zoneOffset    zoneOffset {@link ZoneOffset}
     * @return temporalAccessor
     */
    public static long transDate(String value, DateTimeFormatter timeFormatter, ZoneOffset zoneOffset) {
        long l;
        try {
            l = ZonedDateTime.parse(value, timeFormatter).toInstant().toEpochMilli();
        } catch (DateTimeParseException e1) {
            try {
                l = OffsetDateTime.parse(value, timeFormatter).toInstant().toEpochMilli();
            } catch (DateTimeParseException e2) {
                try {
                    l = LocalDateTime.parse(value, timeFormatter).toEpochSecond(zoneOffset) * 1000L;
                } catch (DateTimeParseException e3) {
                    l = LocalDate.parse(value, timeFormatter).toEpochDay() * 86_400_000L;
                }
            }
        }
        return l;
    }

    /**
     * format date value
     *
     * @param value     value
     * @param formatter time format
     * @param lang      language
     * @return temporalAccessor
     */
    public static TemporalAccessor transDate(String value, String formatter, String lang) {
        return transDate(value, dateTimeFormatter(formatter, lang));
    }
}
