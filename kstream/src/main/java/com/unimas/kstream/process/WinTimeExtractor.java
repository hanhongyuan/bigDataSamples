package com.unimas.kstream.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.unimas.kstream.KsConfig;
import com.unimas.kstream.KsUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Objects;

/**
 * custom time extractor
 */
public class WinTimeExtractor implements TimestampExtractor {

    private final Logger logger = LoggerFactory.getLogger(WinTimeExtractor.class);

    private String time_extractor_name;
    private String zoneOffsetId;
    private DateTimeFormatter dateTimeFormatter = null;
    private ObjectMapper objectMapper;

    public WinTimeExtractor(String time_extractor_name,
                            String time_extractor_value_type,
                            String time_extractor_format,
                            String time_extractor_locale_lang,
                            String time_extractor_zoneOffset_id,
                            ObjectMapper objectMapper) {
        this.time_extractor_name = time_extractor_name;
        if ("string".equals(time_extractor_value_type.toLowerCase())) {
            this.dateTimeFormatter = KsUtils.dateTimeFormatter(
                    KsUtils.nonNullAndEmpty(time_extractor_format, KsConfig.WINDOW_TIME_FORMATTER),
                    time_extractor_locale_lang);
            this.zoneOffsetId = Objects.toString(time_extractor_zoneOffset_id, KsConfig.DEFAULT_DATETIME_ZONEOFFSET_ID);
        }
        this.objectMapper = objectMapper;
    }

    /**
     * Extracts a timestamp from a record. The timestamp must be positive to be considered a valid timestamp.
     * Returning a negative timestamp will cause the record not to be processed but rather silently skipped.
     * In case the record contains a negative timestamp and this is considered a fatal error for the application,
     * throwing a {@link RuntimeException} instead of returning the timestamp is a valid option too.
     * For this case, Streams will stop processing and shut down to allow you investigate in the root cause of the
     * negative timestamp.
     * <p>
     * The timestamp extractor implementation must be stateless.
     * <p>
     * The extracted timestamp MUST represent the milliseconds since midnight, January 1, 1970 UTC.
     * <p>
     * It is important to note that this timestamp may become the message timestamp for any messages sent to changelogs
     * updated by {@link KTable}s and joins.
     * The message timestamp is used for log retention and log rolling, so using nonsensical values may result in
     * excessive log rolling and therefore broker performance degradation.
     *
     * @param record            a data record
     * @param previousTimestamp the latest extracted valid timestamp of the current record's partition˙ (could be -1 if unknown)
     * @return the timestamp of the record
     */
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        try {
            Map val = objectMapper.readValue((String) record.value(), Map.class);
            if (val.containsKey(time_extractor_name)) {
                String value = (String) val.get(time_extractor_name);
                long l;
                if (dateTimeFormatter != null) {
                    l = KsUtils.transDate(value, dateTimeFormatter,
                            ZoneOffset.of(zoneOffsetId));
                } else {
                    if (value.length() != 13) {
                        logger.warn("value: " + record.value() +
                                " time value:" + value + " length not equal 13...");
                        return 0L;
                    }
                    l = Long.valueOf(value);
                }
                return l;
            } else {
                logger.warn("custom time " + time_extractor_name + " does not exit......");
            }
        } catch (IOException | DateTimeParseException | ArithmeticException e) {
            logger.warn(e.getMessage(), e);
        }
        return 0L;
    }
}
