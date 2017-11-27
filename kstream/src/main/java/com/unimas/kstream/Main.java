package com.unimas.kstream;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * main
 */
public class Main {


    public static void main(String[] args) {
        if (args == null || args.length == 0) {
            System.out.printf("Missing configuration parameters...\nusage:%s %s %s\n",
                    "--server<path>",
                    "[--logConf<path>]",
                    "[--logDir<path>]");
            System.exit(0);
        }

        KServer kServer = null;
        Logger logger = null;
        try {

            String ksHome = System.getProperty("ksHome");
            System.setProperty("ks.logs", optionConf(args, "--logDir",
                    ksHome + "/logs"));
            PropertyConfigurator.configure(optionConf(args, "--logConf",
                    ksHome + "/config/log4j.properties"));
            logger = LoggerFactory.getLogger(Main.class);

            String serverFile = getServerConf(args);
            Properties properties = new Properties();
            properties.load(new InputStreamReader(
                    new FileInputStream(serverFile),
                    Charset.forName("UTF-8")));

            KsUtils.nonNullAndEmpty(properties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG),
                    StreamsConfig.APPLICATION_ID_CONFIG);
            KsUtils.nonNullAndEmpty(properties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG),
                    StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

            properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                    properties.getProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                            Serdes.String().getClass().getName()));
            properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                    properties.getProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                            Serdes.String().getClass().getName()));
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                    properties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                            "earliest"));

            CountDownLatch latch = new CountDownLatch(1);
            kServer = new KServer(properties, latch);
            Runtime.getRuntime().addShutdownHook(new Thread(kServer::close));
            logger.info("#################### starting ####################");
            kServer.start();
            latch.await();
        } catch (Throwable e) {
            if (logger != null) {
                logger.error(e.getMessage(), e);
            }
            System.exit(-1);
        } finally {
            if (kServer != null) kServer.close();
        }
        System.exit(0);
    }

    private static String optionConf(String[] args, String opt, String defaultValue) {
        for (int i = 0; i < args.length; i++) {
            if (opt.equals(args[i])) {
                if (i < args.length - 1) {
                    return args[i + 1];
                }
            }
        }
        return defaultValue;
    }

    private static String getServerConf(String[] args) throws Exception {
        for (int i = 0; i < args.length; i++) {
            if ("--server".equals(args[i])) {
                if (i < args.length - 1) {
                    return args[i + 1];
                }
            }
        }
        throw new Exception("--server is not configured...");
    }
}
