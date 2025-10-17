package com.eyelevel.documentprocessor.exception;

import java.io.Serial;

/**
 * An unchecked exception thrown when processing an individual entry from a ZIP archive fails,
 * for instance, when reading its temporary file content.
 */
public class ZipEntryProcessingException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = -144348853764116326L;

    public ZipEntryProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
