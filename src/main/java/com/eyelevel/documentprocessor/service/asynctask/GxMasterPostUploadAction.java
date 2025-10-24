package com.eyelevel.documentprocessor.service.asynctask;

import com.eyelevel.documentprocessor.service.job.JobLifecycleManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Implements the post-upload logic for a GxMaster entity.
 * On success, no action is needed.
 * On failure, it marks the GxMaster record as ERROR.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class GxMasterPostUploadAction implements PostUploadAction {

    private final JobLifecycleManager jobLifecycleManager;

    @Override
    public void onUploadSuccess(Long entityId) {
        log.info("Async S3 upload successful for GxMaster artifact with ID: {}. No further action needed.", entityId);
        // Success for a GxMaster's artifact upload doesn't trigger a new workflow step, so this is intentionally blank.
    }

    @Override
    public void onUploadFailure(Long entityId, Throwable error) {
        log.error("Async S3 upload failed for GxMaster artifact with ID: {}. Marking record as ERROR.", entityId, error);
        jobLifecycleManager.failGxMasterUpload(entityId, "Failed during asynchronous S3 upload: " + error.getMessage());
    }
}
