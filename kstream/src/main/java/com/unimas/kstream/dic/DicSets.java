package com.unimas.kstream.dic;

/**
 * filter sets
 */
public interface DicSets extends Runnable {

    /**
     * Returns <tt>true</tt> if this sets contains the specified element.
     * <p>
     * Returns <tt>true</tt> if this sets is empty.
     *
     * @param name  field name
     * @param value filed value
     * @return <tt>true</tt> if this sets contains the specified element
     */
    boolean contains(String name, String value);

    /**
     * is need to block
     *
     * @return true if this need to wait
     */
    default boolean isBlock() {
        return false;
    }

    /**
     * close impl
     */
    default void shutdown() {

    }
}
