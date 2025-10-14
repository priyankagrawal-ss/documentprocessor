package com.eyelevel.documentprocessor.exception.apiclient;

import java.io.Serial;

/**
 * Exception indicating that a resource was not found (HTTP 404).
 *
 * <p>This exception is thrown when the server cannot find the requested resource.
 */
public class NotFoundException extends ApiException {

    @Serial
    private static final long serialVersionUID = -3051703506470244006L;

    /**
     * Constructs a new NotFoundException with the specified message.
     *
     * @param message A descriptive message about the exception.
     */
    public NotFoundException(String message) {
        super(message, 404);
    }
}