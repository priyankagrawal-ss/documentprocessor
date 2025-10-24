package com.eyelevel.documentprocessor.exception;

import java.io.Serial;

/**
 * A dedicated runtime exception thrown when an SQS message consumer fails
 * to process a message, signaling that the message should be considered for a retry.
 * NOTE: This is an internal exception and should NOT be handled by the GlobalExceptionHandler.
 */
public class MessageProcessingFailedException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 3546738330082948966L;

    public MessageProcessingFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}