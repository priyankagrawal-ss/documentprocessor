package com.eyelevel.documentprocessor.exception;

import java.io.Serial;

public class DocumentProcessingException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 4656352395708234308L;

    public DocumentProcessingException(String message) {
        super(message);
    }

    public DocumentProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}