package com.eyelevel.documentprocessor.exception;

import java.io.Serial;

public class RetryFailedException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    public RetryFailedException(String message) {
        super(message);
    }
}
