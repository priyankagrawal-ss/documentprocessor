package com.eyelevel.documentprocessor.exception.apiclient;

import java.io.Serial;

/**
 * Exception indicating that a conflict occurred (HTTP 409).
 *
 * <p>This exception is thrown when a request conflicts with the current state of the server (e.g.,
 * attempting to create a resource that already exists).
 */
public class ConflictException extends ApiException {

    @Serial
    private static final long serialVersionUID = -5709728403403396930L;

    /**
     * Constructs a new ConflictException with the specified message.
     *
     * @param message A descriptive message about the exception.
     */
    public ConflictException(String message) {
        super(message, 409);
    }
}