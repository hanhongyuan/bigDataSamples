package com.unimas.kstream.dic;

/**
 * 简单的配置分组存储
 *
 */
public class SimpleMap implements DicSets {
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
        return false;
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

    }
}
