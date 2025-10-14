package com.eyelevel.documentprocessor.exception;

import com.eyelevel.documentprocessor.exception.apiclient.ApiException;

import java.io.Serial;

/**
 * Exception indicating that an internal server error occurred (HTTP 500).
 *
 * <p>This exception is thrown when the server encounters an unexpected condition that prevents it
 * from fulfilling the request.
 */
public class InternalServerException extends ApiException {

    @Serial
    private static final long serialVersionUID = 391091864299701366L;

    /**
     * Constructs a new InternalServerException with the specified message.
     *
     * @param message A descriptive message about the exception.
     */
    public InternalServerException(String message) {
        super(message, 500);
    }
}