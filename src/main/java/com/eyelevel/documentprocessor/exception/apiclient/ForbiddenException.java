package com.eyelevel.documentprocessor.exception.apiclient;

import java.io.Serial;

/**
 * Exception indicating that access to a resource is forbidden (HTTP 403).
 *
 * <p>This exception is thrown when the server understands the request, but refuses to authorize
 * it.
 */
public class ForbiddenException extends ApiException {

    @Serial
    private static final long serialVersionUID = 6437220154468580078L;

    /**
     * Constructs a new ForbiddenException with the specified message.
     *
     * @param message A descriptive message about the exception.
     */
    public ForbiddenException(String message) {
        super(message, 403);
    }
}