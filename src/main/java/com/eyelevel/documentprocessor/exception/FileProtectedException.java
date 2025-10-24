package com.eyelevel.documentprocessor.exception;

import java.io.Serial;

/**
 * Thrown when an attempt is made to process a file that is password-protected or encrypted.
 */
public class FileProtectedException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L; // Use a consistent serialVersionUID

    public FileProtectedException(String message) {
        super(message);
    }
}