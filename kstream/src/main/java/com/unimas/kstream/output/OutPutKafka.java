package com.unimas.kstream.output;

import com.unimas.kstream.KsConfig;
import com.unimas.kstream.KServer;
import com.unimas.kstream.KsUtils;
import org.apache.kafka.streams.kstream.KStream;


/**
 * output kafka
 */
public class OutPutKafka extends OutPut {


    public OutPutKafka(KServer kServer) {
        super(kServer.getProperties(), kServer.getObjectMapper());
    }

    /**
     * 输出到目标库
     * 如果输出的value解析错误，则这条记录drop
     *
     * @param kStream 数据流
     */
    @Override
    public void outputImpl(KStream<String, String> kStream) {
        String topicName = KsUtils.nonNullAndEmpty(properties.getProperty(KsConfig.OUTPUT_KAFKA_TOPIC),
                KsConfig.OUTPUT_KAFKA_TOPIC);
        kStream.to(topicName);
    }
}
