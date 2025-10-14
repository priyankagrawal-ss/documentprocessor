package com.eyelevel.documentprocessor.exception.apiclient;

import lombok.Getter;

import java.io.Serial;

/**
 * Base class for API exceptions in the FraudX application.
 *
 * <p>This exception provides a common structure for representing API-related errors, including an
 * HTTP status code and a descriptive message. It is designed to be extended by more specific
 * exception types.
 */
@Getter
public class ApiException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 4830840555831897529L;
    private final int statusCode;

    /**
     * Constructs a new ApiException with the specified message and status code.
     *
     * @param message    A descriptive message about the exception.
     * @param statusCode The HTTP status code associated with the exception.
     */
    public ApiException(String message, int statusCode) {
        super(message);
        this.statusCode = statusCode;
    }
}
