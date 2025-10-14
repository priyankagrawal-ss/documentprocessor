package com.eyelevel.documentprocessor.exception;

import java.io.Serial;

public class FileConversionException extends DocumentProcessingException {
    @Serial
    private static final long serialVersionUID = 5103057382922194402L;

    public FileConversionException(String message) {
        super(message);
    }

    public FileConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}