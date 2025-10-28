package com.eyelevel.documentprocessor.model;

/**
 * Represents the various processing states of a ZIP file in the document processing workflow.
 * <p>
 * Each {@link ZipProcessingStatus} value corresponds to a specific stage in the ZIP extraction lifecycle,
 * from being queued to completion or termination due to errors or manual intervention.
 * </p>
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

    /**
     * The extraction process was forcefully stopped before completion
     * (e.g., due to manual interruption).
     */
    TERMINATED
}
