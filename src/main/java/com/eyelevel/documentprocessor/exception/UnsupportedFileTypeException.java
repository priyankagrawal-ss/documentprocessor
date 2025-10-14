package com.eyelevel.documentprocessor.exception;

import java.io.Serial;

public class UnsupportedFileTypeException extends DocumentProcessingException {
    @Serial
    private static final long serialVersionUID = -4146823766536414925L;

    public UnsupportedFileTypeException(String message) {
        super(message);
    }
}