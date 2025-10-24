package com.eyelevel.documentprocessor.model;

/**
 * Defines the high-level lifecycle states for a ProcessingJob.
 */
public enum ProcessingStatus {
    /**
     * The job has been created, and the system is waiting for the client to upload the file to S3.
     */
    PENDING_UPLOAD,
    /**
     * The client has confirmed the S3 upload, but the job has not yet been queued for backend processing.
     */
    UPLOAD_COMPLETE,
    /**
     * The job has been accepted and is waiting in an SQS queue for a worker to become available.
     */
    QUEUED,
    /**
     * A worker has picked up the job and is actively processing its files.
     */
    PROCESSING,
    /**
     * The job and all its constituent files have been successfully processed and uploaded.
     */
    COMPLETED,
    /**
     * An unrecoverable error occurred at some stage of processing, causing the entire job to fail.
     */
    FAILED, PARTIAL_SUCCESS, TERMINATED
}