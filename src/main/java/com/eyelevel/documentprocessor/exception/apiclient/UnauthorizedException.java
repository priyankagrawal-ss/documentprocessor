package com.eyelevel.documentprocessor.exception.apiclient;

import java.io.Serial;

/**
 * Exception indicating that the request requires user authentication (HTTP 401).
 *
 * <p>This exception is thrown when the request has not been applied because it lacks valid
 * authentication credentials for the target resource.
 */
public class UnauthorizedException extends ApiException {

    @Serial
    private static final long serialVersionUID = -6346735715117211440L;

    /**
     * Constructs a new UnauthorizedException with the specified message.
     *
     * @param message A descriptive message about the exception.
     */
    public UnauthorizedException(String message) {
        super(message, 401);
    }
}