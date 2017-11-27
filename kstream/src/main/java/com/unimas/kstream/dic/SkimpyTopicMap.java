package com.unimas.kstream.dic;

import com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 暂时通过消费者从kafka的topic中获取不多的过滤集
 * 消费者的分组uuid随机生成，避免重启之类后filter集已经消费的问题
 */
public class SkimpyTopicMap implements DicSets {

    private Logger logger = LoggerFactory.getLogger(SkimpyTopicMap.class);

    private AtomicBoolean closed = new AtomicBoolean(false);
    private boolean block = true;
    private ConcurrentMap<String, Set<String>> cache;
    private KafkaConsumer<String, String> consumer;
    private String[] topics;
    private String[] keys;
    private String kafkaAddress;

    public SkimpyTopicMap(String[] topics, String[] keys,
                          String kafkaAddress) {
        this.topics = topics;
        this.keys = keys;
        this.kafkaAddress = kafkaAddress;
        this.cache = new ConcurrentHashMap<>();
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
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                UUID.randomUUID().toString().replace("-", ""));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        consumer = new KafkaConsumer<>(props);

        try {
            consumer.subscribe(ImmutableList.copyOf(topics));
            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                if (records.isEmpty()) block = false;
                for (ConsumerRecord<String, String> record : records) {
                    String topic = record.topic();
                    int index = indexOfTopic(topic);
                    if (index >= 0) {
                        String fileName = keys[index];
                        if (cache.containsKey(fileName)) {
                            cache.get(fileName).add(record.value());
                        } else {
                            Set<String> set = new HashSet<>();
                            set.add(record.value());
                            cache.put(fileName, set);
                        }
                    }
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }
    }

    private int indexOfTopic(String topic) {
        if (topic != null && topics != null) {
            for (int i = 0; i < topics.length; i++) {
                if (topic.equals(topics[i])) return i;
            }
        }
        return -1;
    }

    /**
     * is need to block
     *
     * @return true if this need to wait
     */
    @Override
    public boolean isBlock() {
        return block;
    }

    /**
     * close impl
     */
    @Override
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
        cache = null;
    }

    /**
     * Returns <tt>true</tt> if this sets contains the specified element.
     * <p>
     * Returns <tt>true</tt> if this sets is empty.
     *
     * @param name  field name
     * @param value filed value
     * @return <tt>true</tt> if this sets contains the specified element
     */
    @Override
    public boolean contains(String name, String value) {
        if (cache.isEmpty()) {
            logger.warn("dic sets is empty......result set always true");
            return true;
        } else {
            return cache.containsKey(name) && cache.get(name).contains(value);
        }
    }

}
