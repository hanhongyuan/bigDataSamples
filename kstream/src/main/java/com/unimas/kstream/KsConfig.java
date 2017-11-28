package com.unimas.kstream;


/**
 * config
 */
public class KsConfig {

    public static final String COMMA = ",";
    public static final String SEMICOLON = ";";
    public static final String COLON = ":";

    public static final String DEFAULT_ACTION_ORDER = "mapper,filter,window";
//    public static final String DEFAULT_OUTPUT_KEYS = "all";
    public static final String DEFAULT_DATETIME_LANG = "en";
    public static final String DEFAULT_DATETIME_FORMAT = "uuuu-MM-dd HH:mm:ss.SSS";
    public static final String DEFAULT_DATETIME_ZONEOFFSET_ID = "+08:00";

    public static final String AGG_STORE_NAME = "aggregation";

    public static final String SOURCE_TOPIC_NAME = "source.topic.name";

    public static final String DIC_TYPE = "dic.type";
    public static final String DIC_FIELDS = "dic.fields";
    public static final String DIC_KAFKA_TOPICS = "dic.kafka.topics";
    public static final String DIC_ARRAY_VALUES = "dic.array.values";

    public static final String ACTION_ORDER = "action.order";

    public static final String FILTER_ACTIONS = "filter.actions";
    public static final String FILTER_KEYS = "filter.keys";

    public static final String MAPPER_ACTIONS = "mapper.actions";
    public static final String MAPPER_KEYS = "mapper.keys";
    public static final String MAPPER_APPEND = "mapper.append";

    public static final String WINDOW_KEYS = "window.keys";
    public static final String WINDOW_SIZE = "window.sizeMs";
    public static final String WINDOW_ADVANCE_SIZE = "window.advanceMs";
    public static final String WINDOW_RETENTION_SIZE = "window.retentionMs";
    public static final String WINDOW_COUNT = "window.count";
    public static final String WINDOW_START_TIME = "window.startTime";
    public static final String WINDOW_END_TIME = "window.endTime";
    public static final String WINDOW_STORE_NAME = "window.store.name";
    public static final String WINDOW_TIME_NAME = "window.time.name";
    public static final String WINDOW_TIME_VALUE_TYPE = "window.time.value.type";
    public static final String WINDOW_TIME_FORMATTER = "window.time.formatter";
    public static final String WINDOW_TIME_LOCALE_LANG = "window.time.locale.lang";
    public static final String WINDOW_TIME_OFFSET_ID ="window.time.offsetId";

    public static final String OUTPUT_KEYS = "output.keys";
    public static final String OUTPUT_VALUE_TYPE = "output.value.type";
    public static final String OUTPUT_VALUE_CUSTOM_STYLE = "output.value.custom.style";
    public static final String OUTPUT_TIME_NAME = "output.time.name";
    public static final String OUTPUT_TIME_VALUE_TYPE = "output.time.value.type";
    public static final String OUTPUT_TIME_IN_FORMATTER = "output.time.in.formatter";
    public static final String OUTPUT_TIME_OUT_FORMATTER = "output.time.out.formatter";
    public static final String OUTPUT_TIME_OFFSET_ID = "output.time.offsetId";
    public static final String OUTPUT_TIME_LOCALE_LANG = "output.time.locale.lang";
    public static final String OUTPUT_TYPE = "output.type";
    public static final String OUTPUT_KAFKA_TOPIC = "output.kafka.topic";
    public static final String OUTPUT_ZBUS_ADDRESS = "output.zbus.address";
    public static final String OUTPUT_ZBUS_MQ = "output.zbus.mq";


}
