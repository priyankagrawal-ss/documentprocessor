package com.eyelevel.documentprocessor.service.asynctask;

/**
 * Defines the contract for actions to be executed after an asynchronous S3 upload completes.
 * This follows the Strategy Pattern to make post-upload logic clear and reusable.
 */
public interface PostUploadAction {

    /**
     * The action to perform when the S3 upload is successful.
     *
     * @param entityId The ID of the database entity associated with the upload.
     */
    void onUploadSuccess(Long entityId);

    /**
     * The action to perform when the S3 upload fails.
     *
     * @param entityId The ID of the database entity associated with the upload.
     * @param error    The exception that caused the failure.
     */
    void onUploadFailure(Long entityId, Throwable error);
}
