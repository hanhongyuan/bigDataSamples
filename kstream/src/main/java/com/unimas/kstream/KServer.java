package com.unimas.kstream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.unimas.kstream.dic.DicSets;
import com.unimas.kstream.dic.SkimpyTopicMap;
import com.unimas.kstream.output.OutPut;
import com.unimas.kstream.output.OutPutKafka;
import com.unimas.kstream.output.OutPutZbus;
import com.unimas.kstream.process.WinTimeExtractor;
import com.unimas.kstream.process.v0_11.FilterImpl;
import com.unimas.kstream.process.v0_11.MapperImpl;
import com.unimas.kstream.process.v0_11.WinAggImpl;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * 数据格式解析错误等级为WARN,不会关闭server
 * 配置文件格式错误以及不支持的操作会退出server
 */
public class KServer extends Thread {

    private Logger logger = LoggerFactory.getLogger(KServer.class);

    private Properties properties = null;
    private ObjectMapper objectMapper = null;
    private KafkaStreams streams = null;
    private CountDownLatch stop = null;
    private DicSets dicSets = null;
    private OutPut outPut = null;

    /**
     * internal call
     * do not use it to init server
     *
     * @param properties server property file
     * @param stop       quit tag
     */
    KServer(Properties properties, CountDownLatch stop) {
        this.properties = properties;
        this.stop = stop;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        try {
            if (!Strings.isNullOrEmpty(properties.getProperty(KsConfig.DIC_FIELDS))) {
                this.initDic();
                while (dicSets.isBlock()) {
                    Thread.sleep(1000);
                }
            }
            streams = new KafkaStreams(this.buildStream(), properties);
            streams.start();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            this.close();
        }

    }


    private void initDic() throws Exception {
        logger.info("#################### init dic ####################");
        String type = KsUtils.nonNullAndEmpty(properties.getProperty(KsConfig.DIC_TYPE),
                KsConfig.DIC_TYPE).toLowerCase();
        if (!"kafka".equals(type))
            throw new Exception("dic type " + type + " is not supported...");
        String[] fields = KsUtils.nonNullAndEmpty(properties.getProperty(KsConfig.DIC_FIELDS),
                KsConfig.DIC_FIELDS).split(KsConfig.COMMA);

        //暂时只有kafka实现
        String[] topics = KsUtils.nonNullAndEmpty(properties.getProperty(KsConfig.DIC_KAFKA_TOPICS),
                KsConfig.DIC_KAFKA_TOPICS)
                .split(KsConfig.COMMA);
        if (fields.length != topics.length)
            throw new Exception("dic fields length and topic length is not equal...");

        dicSets = new SkimpyTopicMap(topics, fields,
                properties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        new Thread(dicSets).start();
    }

    /**
     * close resources
     */
    public void close() {
        if (dicSets != null) dicSets.shutdown();
        if (streams != null) streams.close();
        if (outPut != null) outPut.shutdown();
        if (stop != null) stop.countDown();
    }

    private KStreamBuilder buildStream() throws Exception {
        logger.info("#################### build stream ####################");
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> resultStream = this.sourceStream(builder);
        String[] orders = properties.getProperty(KsConfig.ACTION_ORDER, KsConfig.DEFAULT_ACTION_ORDER)
                .split(KsConfig.COMMA);
        for (String order : orders) {
            if ("filter".equals(order.toLowerCase())) {
                if (!Strings.isNullOrEmpty(properties.getProperty(KsConfig.FILTER_KEYS))) {
                    resultStream = this.filterStream(resultStream);
                }
            } else if ("mapper".equals(order.toLowerCase())) {
                if (!Strings.isNullOrEmpty(properties.getProperty(KsConfig.MAPPER_KEYS))) {
                    resultStream = this.mapperStream(resultStream);
                }
            } else if ("window".equals(order.toLowerCase())) {
                if (!Strings.isNullOrEmpty(properties.getProperty(KsConfig.WINDOW_KEYS))) {
                    resultStream = this.windowStream(resultStream);
                }
            } else {
                logger.error("action type " + order + " is not defined...kServer will stop");
                this.close();
            }
        }
        this.outputStream(resultStream);
        return builder;
    }

    private KStream<String, String> sourceStream(KStreamBuilder builder) {
        logger.info("#################### source stream ####################");
        if (Strings.isNullOrEmpty(properties.getProperty(KsConfig.WINDOW_TIME_NAME))) {
            return builder.stream(
                    KsUtils.nonNullAndEmpty(properties.getProperty(KsConfig.SOURCE_TOPIC_NAME),
                            KsConfig.SOURCE_TOPIC_NAME));
        } else {
            return builder.stream(new WinTimeExtractor(
                            properties.getProperty(KsConfig.WINDOW_TIME_NAME),
                            KsUtils.nonNullAndEmpty(
                                    properties.getProperty(KsConfig.WINDOW_TIME_VALUE_TYPE),
                                    KsConfig.WINDOW_TIME_VALUE_TYPE),
                            properties.getProperty(KsConfig.WINDOW_TIME_FORMATTER),
                            properties.getProperty(KsConfig.WINDOW_TIME_LOCALE_LANG),
                            properties.getProperty(KsConfig.WINDOW_TIME_OFFSET_ID),
                            objectMapper
                    ),
                    Serdes.String(),
                    Serdes.String(),
                    KsUtils.nonNullAndEmpty(properties.getProperty(KsConfig.SOURCE_TOPIC_NAME),
                            KsConfig.SOURCE_TOPIC_NAME));
        }
    }


    private KStream<String, String> filterStream(KStream<String, String> kStream) throws Exception {
        logger.info("#################### filter stream ####################");
        Objects.requireNonNull(dicSets, "dic is null(maybe dic not configured)...");
        return new FilterImpl(this).process(kStream);
    }

    private KStream<String, String> mapperStream(KStream<String, String> kStream) {
        logger.info("#################### mapper stream ####################");
        Objects.requireNonNull(dicSets, "dic is null(maybe dic not configured)...");
        return new MapperImpl(this).process(kStream);
    }

    private KStream<String, String> windowStream(KStream<String, String> kStream) {
        logger.info("#################### window stream ####################");
        return new WinAggImpl(this).process(kStream);
    }

    private void outputStream(KStream<String, String> kStream) throws Exception {
        logger.info("#################### output stream ####################");
        String out_type = KsUtils.nonNullAndEmpty(properties.getProperty(KsConfig.OUTPUT_TYPE)
                , KsConfig.OUTPUT_TYPE).toLowerCase();
        if ("kafka".equals(out_type)) {
            outPut = new OutPutKafka(this);
        } else if ("zbus".equals(out_type)) {
            outPut = new OutPutZbus(this);
        } else {
            logger.error("output type " + out_type + " is not supported...kServer will close");
            this.close();
        }
        outPut.output(kStream);
    }

    public Properties getProperties() {
        return properties;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public DicSets getDicSets() {
        return dicSets;
    }
}
