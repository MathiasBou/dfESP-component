package com.sas.o2.cep;

/**
 * Extends {@link RuntimeException} to get a dedicated exception for esp connection lost.
 * @author moritz löser (moritz.loeser@prodyna.com)
 *
 */
public class EspConnectionLostException extends RuntimeException {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public EspConnectionLostException() {
        super();
    }

    public EspConnectionLostException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public EspConnectionLostException(String message, Throwable cause) {
        super(message, cause);
    }

    public EspConnectionLostException(String message) {
        super(message);
    }

    public EspConnectionLostException(Throwable cause) {
        super(cause);
    }

}
