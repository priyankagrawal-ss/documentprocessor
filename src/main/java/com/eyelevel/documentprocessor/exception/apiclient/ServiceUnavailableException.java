package com.eyelevel.documentprocessor.exception.apiclient;

import java.io.Serial;

/**
 * Exception indicating that a service is unavailable (HTTP 503).
 *
 * <p>This exception is thrown when the server is currently unable to handle the request due to a
 * temporary overload or scheduled maintenance, which will be relieved after some delay.
 */
public class ServiceUnavailableException extends ApiException {

    @Serial
    private static final long serialVersionUID = -2812514621225838422L;

    /**
     * Constructs a new ServiceUnavailableException with the specified message.
     *
     * @param message A descriptive message about the exception.
     */
    public ServiceUnavailableException(String message) {
        super(message, 503);
    }
}