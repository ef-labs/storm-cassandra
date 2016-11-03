package com.hmsonline.storm.cassandra.exceptions;

/**
 * Exception indicating that a catastrophic error has occurred,
 * so the topology should be killed (and restarted).
 *
 * @author tgoetz
 *
 */
public class CriticalException extends RuntimeException {

    private static final long serialVersionUID = 6345716882395565541L;

    public CriticalException(String message, Exception e) {
        super(message, e);
    }

}
