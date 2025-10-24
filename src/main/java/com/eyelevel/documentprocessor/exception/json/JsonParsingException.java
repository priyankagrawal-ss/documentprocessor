package com.eyelevel.documentprocessor.exception.json;

import java.io.Serial;

/**
 * Thrown when an attempt to parse or serialize JSON data fails.
 */
public class JsonParsingException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = -4315221486898941505L;

    public JsonParsingException(String message, Throwable cause) {
        super(message, cause);
    }
}
