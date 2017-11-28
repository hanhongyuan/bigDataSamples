package com.unimas.kstream.dic;

import com.google.common.collect.ImmutableSet;
import com.unimas.kstream.KsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 简单的配置分组存储
 * can be used for test
 */
public class SimpleMap implements DicSets {

    private Logger logger = LoggerFactory.getLogger(SimpleMap.class);

    private ConcurrentMap<String, ImmutableSet<String>> cache;
    private String[] fields;
    private String[] values;

    public SimpleMap(String[] fields, String[] values) {
        this.fields = fields;
        this.values = values;
        this.cache = new ConcurrentHashMap<>();
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

    /**
     * is need to block
     *
     * @return true if this need to wait
     */
    @Override
    public boolean isBlock() {
        return false;
    }

    /**
     * close impl
     */
    @Override
    public void shutdown() {

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
        for (int i = 0; i < this.fields.length; i++) {
            String key = this.fields[i];
            String[] values = this.values[i].split(KsConfig.COMMA);
            this.cache.put(key, ImmutableSet.copyOf(values));
        }
    }
}
