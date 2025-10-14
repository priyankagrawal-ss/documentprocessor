package com.eyelevel.documentprocessor.scheduler;

import com.eyelevel.documentprocessor.model.*;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.GxMasterRepository;
import com.eyelevel.documentprocessor.repository.ProcessingJobRepository;
import com.eyelevel.documentprocessor.service.lifecycle.JobLifecycleManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Set;

/**
 * A scheduler that periodically checks the status of all non-terminal processing jobs
 * and updates them to a final state (COMPLETED or FAILED) by delegating to the JobLifecycleManager.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JobLifecycleScheduler {

    private final ProcessingJobRepository processingJobRepository;
    private final FileMasterRepository fileMasterRepository;
    private final GxMasterRepository gxMasterRepository;
    private final JobLifecycleManager jobLifecycleManager;

    private static final Set<GxStatus> FINAL_GX_SUCCESS_STATUSES = Set.of(
            GxStatus.COMPLETE, GxStatus.SKIPPED
    );

    /**
     * Runs on a fixed schedule to find and finalize jobs that are no longer in an active processing state.
     */
    @Scheduled(cron = "${app.scheduler.job-completion-check}")
    public void finalizeJobs() {
        log.info("Starting scheduled job lifecycle finalization check...");

        final List<ProcessingJob> activeJobs = processingJobRepository.findByStatusIn(
                List.of(ProcessingStatus.QUEUED, ProcessingStatus.PROCESSING, ProcessingStatus.UPLOAD_COMPLETE)
        );

        if (CollectionUtils.isEmpty(activeJobs)) {
            log.info("No active jobs found to finalize. Scheduler run is complete.");
            return;
        }

        log.info("Found {} active jobs to check for finalization.", activeJobs.size());
        for (final ProcessingJob job : activeJobs) {
            try {
                determineJobFinalState(job);
            } catch (Exception e) {
                log.error("Error during finalization check for Job ID: {}. It will be re-checked on the next run.", job.getId(), e);
            }
        }
        log.info("Finished scheduled job lifecycle check.");
    }

    /**
     * Determines the final state of a single job by inspecting all its child entities.
     */
    private void determineJobFinalState(final ProcessingJob job) {
        final long jobId = job.getId();

        final List<FileMaster> associatedFiles = fileMasterRepository.findAllByProcessingJobId(jobId);
        if (associatedFiles.isEmpty()) {
            log.debug("Job ID {} has no associated files yet. Skipping for now.", jobId);
            return;
        }

        final boolean hasPendingFiles = associatedFiles.stream().anyMatch(
                file -> file.getFileProcessingStatus() == FileProcessingStatus.QUEUED ||
                        file.getFileProcessingStatus() == FileProcessingStatus.IN_PROGRESS
        );
        if (hasPendingFiles) {
            log.debug("Job ID {} still has files in active processing. Will check again later.", jobId);
            return;
        }

        final List<GxMaster> associatedGxRecords = gxMasterRepository.findAllByProcessingJobId(jobId);
        for (final GxMaster gxRecord : associatedGxRecords) {
            if (gxRecord.getGxStatus() == GxStatus.ERROR) {
                String errorMessage = "GX upload failed for file '%s'. Reason: %s".formatted(
                        gxRecord.getProcessedFileName(), gxRecord.getErrorMessage());
                jobLifecycleManager.failJob(jobId, errorMessage);
                return;
            }
        }

        final long expectedGxRecords = associatedFiles.stream()
                .filter(file -> file.getFileProcessingStatus() == FileProcessingStatus.COMPLETED)
                .count();
        if (associatedGxRecords.size() < expectedGxRecords) {
            log.debug("Job ID {} is awaiting creation of all GxMaster records (expected {}, found {}).",
                    jobId, expectedGxRecords, associatedGxRecords.size());
            return;
        }

        final boolean allGxRecordsAreFinal = associatedGxRecords.stream()
                .allMatch(gx -> FINAL_GX_SUCCESS_STATUSES.contains(gx.getGxStatus()));
        if (allGxRecordsAreFinal) {
            log.info("All components for Job ID {} have completed successfully. Marking job as COMPLETED.", jobId);
            jobLifecycleManager.completeJob(jobId);
        } else {
            log.debug("Job ID {} is still awaiting completion of its GX processing.", jobId);
        }
    }
}