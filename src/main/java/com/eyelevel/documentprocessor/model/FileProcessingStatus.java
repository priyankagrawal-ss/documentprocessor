package com.eyelevel.documentprocessor.model;

public enum FileProcessingStatus {
    QUEUED,
    IN_PROGRESS,
    COMPLETED,
    FAILED,
    SKIPPED_DUPLICATE,
    IGNORED
}