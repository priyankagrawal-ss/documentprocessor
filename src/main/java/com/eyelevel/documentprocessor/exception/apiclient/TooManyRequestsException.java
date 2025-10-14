package com.eyelevel.documentprocessor.exception.apiclient;

import java.io.Serial;

/**
 * Exception indicating that too many requests were made (HTTP 429).
 *
 * <p>This exception is thrown when the user has sent too many requests in a given amount of time.
 */
public class TooManyRequestsException extends ApiException {

    @Serial
    private static final long serialVersionUID = -6576126133407459351L;

    /**
     * Constructs a new TooManyRequestsException with the specified message.
     *
     * @param message A descriptive message about the exception.
     */
    public TooManyRequestsException(String message) {
        super(message, 429);
    }
}