package com.eyelevel.documentprocessor.model;

/**
 * Defines the possible states for a FileMaster record during its processing lifecycle.
 */
public enum FileProcessingStatus {
    /**
     * The file has been identified and is waiting in a queue to be processed.
     */
    QUEUED,
    /**
     * The file has been picked up by a worker and is actively being processed.
     */
    IN_PROGRESS,
    /**
     * The file has been successfully processed through the pipeline.
     */
    COMPLETED,
    /**
     * An unrecoverable error occurred during processing.
     */
    FAILED,
    /**
     * Processing was skipped because the file's content was identical to an already completed file.
     */
    SKIPPED_DUPLICATE,
    /**
     * Processing was skipped due to a validation error (e.g., zero-byte file, unsupported type).
     */
    IGNORED
}