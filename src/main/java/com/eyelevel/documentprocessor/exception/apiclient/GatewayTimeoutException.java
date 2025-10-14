package com.eyelevel.documentprocessor.exception.apiclient;

import java.io.Serial;

/**
 * Exception indicating that a gateway timeout occurred (HTTP 504).
 *
 * <p>This exception is thrown when the server, while acting as a gateway or proxy, did not receive
 * a timely response from another server.
 */
public class GatewayTimeoutException extends ApiException {

    @Serial
    private static final long serialVersionUID = -7071019530397087493L;

    /**
     * Constructs a new GatewayTimeoutException with the specified message.
     *
     * @param message A descriptive message about the exception.
     */
    public GatewayTimeoutException(String message) {
        super(message, 504);
    }
}