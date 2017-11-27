package com.unimas.kstream.output;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.unimas.kstream.KsConfig;
import com.unimas.kstream.KsUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 输出端
 */
public abstract class OutPut {

    private final char sub = '$';
    private final Logger logger = LoggerFactory.getLogger(OutPut.class);

    Properties properties;
    private ObjectMapper objectMapper;

    private boolean first = true;

    private ImmutableList<String> outputFields;

    private boolean formatVal;
    private ImmutableList<ImmutableTuple<Boolean, String>> customStyles;


    private boolean formatDate;
    private ImmutableList<String> time_fields;
    private ImmutableList<String> time_types;
    private ImmutableList<String> time_in_formats;
    private String time_out_format;
    private String time_out_zoneOffsetId;
    private String time_out_lang;

    OutPut(Properties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        String outputField = properties.getProperty(KsConfig.OUTPUT_KEYS, "all").toLowerCase();
        if (!"all".equals(outputField)) {
            this.outputFields = ImmutableList.copyOf(outputField.split(KsConfig.COMMA));
        }
    }


    /**
     * output custom value
     * <p>
     * format defined time field
     *
     * @param valM output value
     * @return custom value
     */
    @SuppressWarnings("unchecked")
    private String formatValue(Map valM) throws IOException {
        if (first) {
            first = false;
            this.formatVal = properties.getProperty(KsConfig.OUTPUT_VALUE_TYPE, "json")
                    .toLowerCase().equals("custom");
            if (this.formatVal) {
                List<ImmutableTuple<Boolean, String>> list = new ArrayList<>();
                char[] valueStyles = KsUtils.nonNullAndEmpty(
                        properties.getProperty(KsConfig.OUTPUT_VALUE_CUSTOM_STYLE),
                        KsConfig.OUTPUT_VALUE_CUSTOM_STYLE).toCharArray();
                int tmp = 0;
                for (int i = 0; i < valueStyles.length; i++) {
                    char c = valueStyles[i];
                    if (sub == c) {
                        String slice = String.valueOf(Arrays.copyOfRange(valueStyles, tmp, i));
                        list.add(ImmutableTuple.of(valM.containsKey(slice), slice));
                        tmp = i + 1;
                    }
                }
                String tail = String.valueOf(Arrays.copyOfRange(valueStyles, tmp, valueStyles.length));
                list.add(ImmutableTuple.of(valM.containsKey(tail), tail));
                this.customStyles = ImmutableList.copyOf(list);
            }
            this.formatDate = !Strings.isNullOrEmpty(properties.getProperty(KsConfig.OUTPUT_TIME_NAME));
            if (this.formatDate) {
                this.time_fields = ImmutableList.copyOf(KsUtils.nonNullAndEmpty(
                        properties.getProperty(KsConfig.OUTPUT_TIME_NAME),
                        KsConfig.OUTPUT_TIME_NAME).split(KsConfig.COMMA));
                this.time_types = ImmutableList.copyOf(KsUtils.nonNullAndEmpty(
                        properties.getProperty(KsConfig.OUTPUT_TIME_VALUE_TYPE),
                        KsConfig.OUTPUT_TIME_VALUE_TYPE).split(KsConfig.COMMA));
                if (properties.getProperty(KsConfig.OUTPUT_TIME_VALUE_TYPE).toLowerCase().contains("string")) {
                    this.time_in_formats = ImmutableList.copyOf(KsUtils.nonNullAndEmpty(
                            properties.getProperty(KsConfig.OUTPUT_TIME_IN_FORMATTER),
                            KsConfig.OUTPUT_TIME_IN_FORMATTER).split(KsConfig.COMMA));
                }
                this.time_out_format = properties.getProperty(KsConfig.OUTPUT_TIME_OUT_FORMATTER,
                        KsConfig.DEFAULT_DATETIME_FORMAT);
                this.time_out_zoneOffsetId = properties.getProperty(KsConfig.OUTPUT_TIME_OFFSET_ID,
                        KsConfig.DEFAULT_DATETIME_ZONEOFFSET_ID);
                this.time_out_lang = properties.getProperty(KsConfig.OUTPUT_TIME_LOCALE_LANG,
                        KsConfig.DEFAULT_DATETIME_LANG);
            }
        }
        if (formatVal) {
            StringBuilder stringBuilder = new StringBuilder();
            customStyles.forEach(tuple -> {
                if (tuple.getLeft()) {
                    String field = tuple.getRight();
                    String value = (String) valM.get(field);
                    if (formatDate && time_fields.contains(field)) {
                        value = this.formatDateValue(field, value);
                    }
                    stringBuilder.append(value);
                } else {
                    stringBuilder.append(tuple.getRight());
                }
            });
            return stringBuilder.toString();
        } else if (formatDate) {
            ImmutableMap tmp = ImmutableMap.copyOf(valM);
            tmp.forEach((k, v) -> {
                String field = (String) k;
                String value = (String) valM.get(field);
                if (time_fields.contains(field)) {
                    value = this.formatDateValue(field, value);
                    valM.put(field, value);
                }
            });
            return objectMapper.writeValueAsString(valM);
        }
        return objectMapper.writeValueAsString(valM);
    }

    /**
     * format date value
     *
     * @param field key
     * @param value value
     * @return converted date value
     */
    private String formatDateValue(String field, String value) {
        int index = time_fields.indexOf(field);
        String type = time_types.get(index);
        TemporalAccessor temporalAccessor;
        if ("string".equals(type.toLowerCase())) {
            temporalAccessor = KsUtils.transDate(value, time_in_formats.get(index), time_out_lang);
        } else {
            temporalAccessor = Instant.ofEpochMilli(Long.valueOf(value)).atOffset(
                    ZoneOffset.of(time_out_zoneOffsetId));
        }
        return KsUtils.dateTimeFormatter(time_out_format, time_out_lang).format(temporalAccessor);
    }

    /**
     * 输出到目标库
     *
     * @param kStream 数据流
     */
    @SuppressWarnings("unchecked")
    public void output(KStream<String, String> kStream) {
        this.outputImpl(kStream.mapValues(value -> {
            try {
                Map valM = objectMapper.readValue(value, Map.class);
                if (outputFields != null) {
                    ImmutableMap.Builder<String, String> resultBuilder = ImmutableMap.builder();
                    for (String field : outputFields) {
                        if (valM.containsKey(field)) {
                            resultBuilder.put(field, (String) valM.get(field));
                        } else {
                            resultBuilder.put(field, "null");
                        }
                    }
                    valM = resultBuilder.build();
                }
                return this.formatValue(valM);
            } catch (IOException e) {
                logger.warn(value, e);
            }
            return null;
        }));
    }

    /**
     * close impl
     */
    public void shutdown() {
    }

    /**
     * 输出到目标库
     *
     * @param kStream 数据流
     */
    public abstract void outputImpl(KStream<String, String> kStream);


//    public static void main(String[] args) {
//        Properties properties = new Properties();
//        try {
//            properties.load(new InputStreamReader(
//                    new FileInputStream("/home/ethan/projects/kstream/config/template.properties"),
//                    Charset.forName("utf-8")));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        OutPut outPut = new OutPut(properties, new ObjectMapper()) {
//            /**
//             * 输出到目标库
//             *
//             * @param kStream 数据流
//             */
//            @Override
//            public void outputImpl(KStream<String, String> kStream) {
//
//            }
//        };
//
//        try {
//            for (int i = 0; i < 10; i++) {
//                String count = String.valueOf(i);
//                Map map = new HashMap();
//                map.put("count", count);
//                map.put("start", "1511508035000");
//                map.put("end", "2017-11-24 15:20:35.000");
//                System.out.println(outPut.formatValue(map));
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//    }
}
