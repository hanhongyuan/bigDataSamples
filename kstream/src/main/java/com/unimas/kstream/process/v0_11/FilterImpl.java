package com.unimas.kstream.process.v0_11;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.unimas.kstream.KsConfig;
import com.unimas.kstream.KServer;
import com.unimas.kstream.KsUtils;
import com.unimas.kstream.dic.DicSets;
import com.unimas.kstream.process.Processor;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * filter impl
 */
public class FilterImpl implements Processor {

    private final Logger logger = LoggerFactory.getLogger(FilterImpl.class);

    private KServer kServer;

    public FilterImpl(KServer kServer) {
        this.kServer = kServer;
    }


    /**
     * stream process
     * 如果value值格式化出错，则filter无效，返回true
     * 如果action类型暂不支持，则filter无效，返回true
     *
     * @param kStream 待处理的数据流
     * @return kstream {@link KStream}
     */
    @Override
    public KStream<String, String> process(KStream<String, String> kStream) {
        Properties properties = kServer.getProperties();
        String[] actions = KsUtils.nonNullAndEmpty(properties.getProperty(KsConfig.FILTER_ACTIONS),
                KsConfig.FILTER_ACTIONS).split(KsConfig.COMMA);
        String[] keys = KsUtils.nonNullAndEmpty(properties.getProperty(KsConfig.FILTER_KEYS), KsConfig.FILTER_KEYS)
                .split(KsConfig.COMMA);
        if (actions.length != keys.length) {
            logger.error("filter action length and key length is not equal...kServer will close");
            if (kServer != null) kServer.close();
        }

        ObjectMapper objectMapper = kServer.getObjectMapper();
        DicSets dicSets = kServer.getDicSets();
        return kStream.filter((k, v) -> {
            try {
                Map valM = objectMapper.readValue(v, Map.class);
                boolean result = true;
                for (int i = 0; i < keys.length; i++) {
                    String filedName = keys[i];
                    String action = actions[i].toLowerCase();
                    if (valM.containsKey(filedName)) {
                        if ("in".equals(action)) {
                            result = result && dicSets.contains(filedName, (String) valM.get(filedName));
                        } else if ("notin".equals(action)) {
                            result = result && !dicSets.contains(filedName, (String) valM.get(filedName));
                        } else {
                            logger.error("filter action " + action + " is not supported...kServer will close");
                            if (kServer != null) kServer.close();
                        }
                    }
                }
                return result;
            } catch (Exception e) {
                logger.warn(v, e);
            }
            return true;
        });
    }
}
