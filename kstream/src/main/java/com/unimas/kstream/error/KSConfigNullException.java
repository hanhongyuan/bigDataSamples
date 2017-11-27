package com.unimas.kstream.error;


/**
 * The configuration element is null or empty
 * this exception will stop kServer
 */
public class KSConfigNullException extends RuntimeException {

    /**
     * Constructs a {@code KSConfigNullException} with the specified
     * detail message.
     *
     * @param s the detail message.
     */
    public KSConfigNullException(String s) {
        super(s + " is null or empty and will close kServer...");
    }


}
