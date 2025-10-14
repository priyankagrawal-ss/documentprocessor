package com.eyelevel.documentprocessor.exception.json;

import java.io.Serial;

/**
 * Exception indicating that there was an error parsing JSON data.
 *
 * <p>This exception is thrown when an attempt to parse JSON data fails.
 */
public class JsonParsingException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = -4315221486898941505L;

    /**
     * Constructs a new JsonParsingException with the specified message and cause.
     *
     * @param message A descriptive message about the exception.
     * @param cause   The underlying cause of the exception.
     */
    public JsonParsingException(String message, Throwable cause) {
        super(message, cause);
    }
}
