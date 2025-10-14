package com.eyelevel.documentprocessor.exception.apiclient;

import java.io.Serial;

/**
 * Exception indicating that a bad gateway error occurred (HTTP 502).
 *
 * <p>This exception is thrown when the server, while acting as a gateway or proxy, received an
 * invalid response from another server.
 */
public class BadGatewayException extends ApiException {

    @Serial
    private static final long serialVersionUID = -318246245731973719L;

    /**
     * Constructs a new BadGatewayException with the specified message.
     *
     * @param message A descriptive message about the exception.
     */
    public BadGatewayException(String message) {
        super(message, 502);
    }
}