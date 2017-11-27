package com.unimas.kstream.process;

import org.apache.kafka.streams.kstream.KStream;

/**
 * 内部的serde为string
 * 原始topic的value存储msg，key可以设置以利于kafka的partition分配
 */
public interface Processor {


    /**
     * stream process
     *
     * @param kStream 待处理的数据流
     * @return kstream {@link KStream}
     */
    KStream<String, String> process(KStream<String, String> kStream);
}
