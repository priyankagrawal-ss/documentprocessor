package com.eyelevel.documentprocessor.model;

/**
 * Defines the possible states for a ZipMaster record during the extraction phase.
 */
public enum ZipProcessingStatus {
    /**
     * The ZIP file is waiting in a queue to be extracted.
     */
    QUEUED_FOR_EXTRACTION,
    /**
     * A worker is actively extracting files from the ZIP archive.
     */
    EXTRACTION_IN_PROGRESS,
    /**
     * All files were successfully extracted from the ZIP archive.
     */
    EXTRACTION_SUCCESS,
    /**
     * An error occurred during extraction (e.g., corrupt file, invalid structure).
     */
    EXTRACTION_FAILED,
    TERMINATED
}