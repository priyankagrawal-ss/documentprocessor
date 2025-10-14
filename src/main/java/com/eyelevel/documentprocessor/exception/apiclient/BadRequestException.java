package com.eyelevel.documentprocessor.exception.apiclient;

import java.io.Serial;

/**
 * Exception indicating that a bad request error occurred (HTTP 400).
 *
 * <p>This exception is thrown when the server cannot or will not process the request due to
 * something that is perceived to be a client error (e.g., malformed request syntax, invalid request
 * message framing, or deceptive request routing).
 */
public class BadRequestException extends ApiException {

    @Serial
    private static final long serialVersionUID = -4414516763190851688L;

    /**
     * Constructs a new BadRequestException with the specified message.
     *
     * @param message A descriptive message about the exception.
     */
    public BadRequestException(String message) {
        super(message, 400);
    }
}