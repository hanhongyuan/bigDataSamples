package com.unimas.kstream.process.v0_11;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.unimas.kstream.KServer;
import com.unimas.kstream.KsConfig;
import com.unimas.kstream.KsUtils;
import com.unimas.kstream.dic.DicSets;
import com.unimas.kstream.process.Processor;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * mapper impl
 */
public class MapperImpl implements Processor {

    private Logger logger = LoggerFactory.getLogger(MapperImpl.class);
    private KServer kServer;

    public MapperImpl(KServer kServer) {
        this.kServer = kServer;
    }

    /**
     * stream process
     * <p>
     * 如果json转换出错，这条记录drop
     * <p>
     * 映射转换后value添加配置键值对
     *
     * @param kStream 待处理的数据流
     * @return kstream {@link KStream}
     */
    @SuppressWarnings("unchecked")
    @Override
    public KStream<String, String> process(KStream<String, String> kStream) {
        Properties properties = kServer.getProperties();
        ObjectMapper objectMapper = kServer.getObjectMapper();
        DicSets dicSets = kServer.getDicSets();
        String[] action_con = KsUtils.nonNullAndEmpty(properties.getProperty(KsConfig.MAPPER_ACTIONS),
                KsConfig.MAPPER_ACTIONS).split(KsConfig.SEMICOLON);
        String[] key_con = KsUtils.nonNullAndEmpty(properties.getProperty(KsConfig.MAPPER_KEYS),
                KsConfig.MAPPER_KEYS).split(KsConfig.SEMICOLON);
        String[] appends = KsUtils.nonNullAndEmpty(properties.getProperty(KsConfig.MAPPER_APPEND),
                KsConfig.MAPPER_APPEND).split(KsConfig.SEMICOLON);

        if (action_con.length != key_con.length || action_con.length != appends.length) {
            logger.error("action length,key length,append length is not equal...kServer will close");
            if (kServer != null) kServer.close();
        }
        return kStream.mapValues(value -> {
            try {
                Map valM = objectMapper.readValue(value, Map.class);
                ImmutableMap.Builder<String, String> valBuild = ImmutableMap.builder();
                for (int i = 0; i < appends.length; i++) {
                    String[] actions = action_con[i].split(KsConfig.COMMA);
                    String[] keys = key_con[i].split(KsConfig.COMMA);
                    boolean result = true;
                    for (int j = 0; j < keys.length; j++) {
                        String filedName = keys[j];
                        String action = actions[j].toLowerCase();
                        if (valM.containsKey(filedName)) {
                            if ("in".equals(action)) {
                                result = result && dicSets.contains(filedName, (String) valM.get(filedName));
                            } else if ("notin".equals(action)) {
                                result = result && !dicSets.contains(filedName, (String) valM.get(filedName));
                            } else {
                                logger.error("mapper action " + action + " is not supported...kServer will close");
                                if (kServer != null) kServer.close();
                            }
                        }
                    }
                    if (result) {
                        String[] kv = appends[i].split(KsConfig.COLON);
                        if (kv.length <= 1) {
                            logger.error("mapper append value " + appends[i] + " does not contain colon(:),kServer will close");
                            if (kServer != null) kServer.close();
                        }
                        valBuild.put(kv[0], kv[1]);
                    }
                }
                valM.putAll(valBuild.build());
                return objectMapper.writeValueAsString(valM);
            } catch (IOException e) {
                logger.warn(value, e);
            }
            return null;
        });
    }
}
