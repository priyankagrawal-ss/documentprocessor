package com.eyelevel.documentprocessor.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * A simple data holder for raw file content extracted by a handler.
 * We use a byte array because the source InputStream will be closed.
 */
@Getter
@AllArgsConstructor
public class ExtractedFileItem {
    private final String filename;
    private final byte[] content;
}