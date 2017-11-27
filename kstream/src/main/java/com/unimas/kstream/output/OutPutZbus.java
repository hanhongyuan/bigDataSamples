package com.unimas.kstream.output;

import com.unimas.kstream.KsConfig;
import com.unimas.kstream.KServer;
import com.unimas.kstream.KsUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zbus.broker.Broker;
import org.zbus.broker.ZbusBroker;
import org.zbus.mq.Producer;
import org.zbus.net.http.Message;

import java.io.IOException;

/**
 * output zbus
 */
public class OutPutZbus extends OutPut {

    private final Logger logger = LoggerFactory.getLogger(OutPutZbus.class);
    private KServer kServer;
    private Broker broker;

    public OutPutZbus(KServer kServer) {
        super(kServer.getProperties(), kServer.getObjectMapper());
        this.kServer = kServer;
    }

    /**
     * 输出到目标库
     *
     * @param kStream 数据流
     */
    @Override
    public void outputImpl(KStream<String, String> kStream) {
        String address = KsUtils.nonNullAndEmpty(properties.getProperty(KsConfig.OUTPUT_ZBUS_ADDRESS),
                KsConfig.OUTPUT_ZBUS_ADDRESS);
        String mq = KsUtils.nonNullAndEmpty(properties.getProperty(KsConfig.OUTPUT_ZBUS_MQ),
                KsConfig.OUTPUT_ZBUS_MQ);
        try {
            broker = new ZbusBroker(address);
            Producer producer = new Producer(broker, mq);
            producer.createMQ();
            kStream.foreach((k, v) -> {
                try {
                    producer.sendSync(new Message(v));
                } catch (IOException | InterruptedException e) {
                    logger.warn(v, e);
                }
            });
        } catch (IOException | InterruptedException e) {
            logger.error(e.getMessage(), e);
            if (kServer != null) kServer.close();
        }
    }

    /**
     * close impl
     */
    @Override
    public void shutdown() {
        try {
            if (broker != null) {
                broker.close();
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        broker = null;
    }
}
