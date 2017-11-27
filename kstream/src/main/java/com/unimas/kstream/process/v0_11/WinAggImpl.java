package com.unimas.kstream.process.v0_11;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.unimas.kstream.KsConfig;
import com.unimas.kstream.KServer;
import com.unimas.kstream.KsUtils;
import com.unimas.kstream.process.Processor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * window aggregation impl
 */
public class WinAggImpl implements Processor {

    private Logger logger = LoggerFactory.getLogger(WinAggImpl.class);
    private KServer kServer;

    public WinAggImpl(KServer kServer) {
        this.kServer = kServer;
    }

    /**
     * stream process
     * <p>
     * 处理后的value增加count,window_start,window_end三个属性列
     * 处理后的key为分组属性列组成的json字符串
     * <p>
     * 如果value中没有任何一个group key,则这条记录drop
     * 如果json转换出错，这条记录drop
     *
     * @param kStream 待处理的数据流
     * @return kstream {@link KStream}
     */
    @SuppressWarnings("unchecked")
    @Override
    public KStream<String, String> process(KStream<String, String> kStream) {
        Properties properties = kServer.getProperties();
        ObjectMapper objectMapper = kServer.getObjectMapper();

        String window_size_s = properties.getProperty(KsConfig.WINDOW_SIZE, "600000");
        long window_size = Long.valueOf(window_size_s);
        long advance_size = Long.valueOf(properties.getProperty(KsConfig.WINDOW_ADVANCE_SIZE, window_size_s));
        long retention_size_temp = Long.valueOf(properties.getProperty(KsConfig.WINDOW_RETENTION_SIZE, window_size_s));
        long retention_size = retention_size_temp;
        if (retention_size_temp < window_size) {
            logger.warn("window.retention.size is lower than window.size and retention size will reset to window size");
            retention_size = window_size;
        }

        String count_field = KsUtils.nonNullAndEmpty(properties.getProperty(KsConfig.WINDOW_COUNT),
                KsConfig.WINDOW_COUNT);
        ImmutableList<String> groupKeys = ImmutableList.copyOf(
                KsUtils.nonNullAndEmpty(properties.getProperty(KsConfig.WINDOW_KEYS),
                        KsConfig.WINDOW_KEYS).split(KsConfig.COMMA));

        return kStream
                .map((KeyValueMapper<String, String, KeyValue<String, String>>) (key, value) -> {
                    try {
                        Map mapVal = objectMapper.readValue(value, Map.class);
                        ImmutableMap.Builder<String, Object> krBuilder = ImmutableMap.builder();
                        groupKeys.forEach(k -> {
                            if (mapVal.containsKey(k)) {
                                krBuilder.put(k, mapVal.get(k));
                            }
                        });
                        ImmutableMap<String, Object> kr = krBuilder.build();
                        if (!kr.isEmpty()) return new KeyValue<>(objectMapper.writeValueAsString(kr), value);
                        return new KeyValue<>(null, null); //如果value中没有任何一个group key,则这条记录drop
                    } catch (IOException e) {
                        logger.warn(value, e);
                    }
                    return new KeyValue<>(null, null);
                })
                .groupByKey()
                .aggregate(() -> "{\"" + count_field + "\":\"0\"}",
                        (key, value, aggregate) -> {
                            try {
                                Map agg = objectMapper.readValue(aggregate, Map.class);
                                Map mapVal = objectMapper.readValue(value, Map.class);
                                if (!mapVal.containsKey(count_field) && agg.containsKey(count_field)) {
                                    int pre = Integer.parseInt(String.valueOf(agg.get(count_field)));
                                    mapVal.put(count_field, (pre + 1) + "");
                                } else {
                                    int pre = Integer.parseInt(String.valueOf(mapVal.get(count_field)));
                                    mapVal.put(count_field, (pre + 1) + "");
                                }
                                return objectMapper.writeValueAsString(mapVal);
                            } catch (IOException e) {
                                logger.warn(value, e);
                            }
                            return null;
                        },
                        TimeWindows.of(window_size).advanceBy(advance_size).until(retention_size),
                        Serdes.String(),
                        properties.getProperty(KsConfig.WINDOW_STORE_NAME, KsConfig.AGG_STORE_NAME))
                .toStream()
                .map((key, value) -> {
                    Window windowKey = key.window();
                    try {
                        if (Strings.isNullOrEmpty(properties.getProperty(KsConfig.WINDOW_START_TIME)) &&
                                Strings.isNullOrEmpty(properties.getProperty(KsConfig.WINDOW_END_TIME))) {
                            return new KeyValue<>(key.key(), value);
                        } else {
                            ImmutableMap.Builder<String, String> appendBuilder = ImmutableMap.builder();
                            if (!Strings.isNullOrEmpty(properties.getProperty(KsConfig.WINDOW_START_TIME))) {
                                appendBuilder.put(properties.getProperty(KsConfig.WINDOW_START_TIME),
                                        String.valueOf(windowKey.start()));
                            }
                            if (!Strings.isNullOrEmpty(properties.getProperty(KsConfig.WINDOW_END_TIME))) {
                                appendBuilder.put(properties.getProperty(KsConfig.WINDOW_END_TIME),
                                        String.valueOf(windowKey.end()));
                            }
                            ImmutableMap.Builder resultVal = ImmutableMap.builder()
                                    .putAll(objectMapper.readValue(value, Map.class))
                                    .putAll(appendBuilder.build());
                            return new KeyValue<>(key.key(),
                                    objectMapper.writeValueAsString(resultVal.build()));
                        }
                    } catch (IOException e) {
                        logger.warn(value, e);
                    }
                    return new KeyValue<>(null, null);
                });
    }
}
