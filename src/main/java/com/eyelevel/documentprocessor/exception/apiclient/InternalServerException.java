package com.eyelevel.documentprocessor.exception.apiclient;

import java.io.Serial;

/**
 * Exception indicating that an internal server error occurred (HTTP 500).
 */
public class InternalServerException extends ApiException {
    @Serial
    private static final long serialVersionUID = 391091864299701366L;

    public InternalServerException(String message) {
        super(message, 500);
    }
}