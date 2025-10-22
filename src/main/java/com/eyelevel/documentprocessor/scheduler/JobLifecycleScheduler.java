package com.eyelevel.documentprocessor.scheduler;

import com.eyelevel.documentprocessor.model.*;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.GxMasterRepository;
import com.eyelevel.documentprocessor.repository.ProcessingJobRepository;
import com.eyelevel.documentprocessor.repository.ZipMasterRepository;
import com.eyelevel.documentprocessor.service.lifecycle.JobLifecycleManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A scheduler that periodically checks the status of all non-terminal processing jobs
 * and updates them to a final state (COMPLETED, FAILED, or PARTIAL_SUCCESS).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JobLifecycleScheduler {

    private static final Set<ProcessingStatus> ACTIVE_STATUSES = Set.of(ProcessingStatus.QUEUED,
                                                                        ProcessingStatus.PROCESSING,
                                                                        ProcessingStatus.UPLOAD_COMPLETE);
    private static final Set<ZipProcessingStatus> PENDING_ZIP_STATUSES = Set.of(
            ZipProcessingStatus.QUEUED_FOR_EXTRACTION, ZipProcessingStatus.EXTRACTION_IN_PROGRESS);
    private static final Set<FileProcessingStatus> PENDING_FILE_STATUSES = Set.of(FileProcessingStatus.QUEUED,
                                                                                  FileProcessingStatus.IN_PROGRESS);
    private static final Set<GxStatus> PENDING_GX_STATUSES = Set.of(GxStatus.QUEUED_FOR_UPLOAD, GxStatus.PROCESSING);
    private final ProcessingJobRepository processingJobRepository;
    private final ZipMasterRepository zipMasterRepository;
    private final FileMasterRepository fileMasterRepository;
    private final GxMasterRepository gxMasterRepository;
    private final JobLifecycleManager jobLifecycleManager;

    /**
     * Runs on a fixed schedule to find and finalize jobs that are no longer in an active processing state.
     */
    @Scheduled(cron = "${app.scheduler.job-completion-check}")
    public void finalizeJobs() {
        log.info("Starting scheduled job lifecycle finalization check...");
        final List<ProcessingJob> activeJobs = processingJobRepository.findByStatusIn(ACTIVE_STATUSES);

        if (activeJobs.isEmpty()) {
            log.info("No active jobs found to finalize. Scheduler run is complete.");
            return;
        }

        log.info("Found {} active jobs to check for finalization.", activeJobs.size());
        for (final ProcessingJob job : activeJobs) {
            try {
                determineJobFinalState(job);
            } catch (Exception e) {
                log.error("Error during finalization check for Job ID: {}. It will be re-checked on the next run.",
                          job.getId(), e);
            }
        }
        log.info("Finished scheduled job lifecycle check.");
    }

    /**
     * Determines the final state of a single job by summarizing all its child entities.
     * This version uses precise logic to differentiate between COMPLETED, FAILED,
     * and PARTIAL_SUCCESS states based on the specific mix of file outcomes.
     */
    private void determineJobFinalState(final ProcessingJob job) {
        final long jobId = job.getId();

        final List<ZipMaster> associatedZips = zipMasterRepository.findAllByProcessingJobId(jobId);
        final List<FileMaster> associatedFiles = fileMasterRepository.findAllByProcessingJobId(jobId);
        final List<GxMaster> associatedGxRecords = gxMasterRepository.findAllByProcessingJobId(jobId);

        // Priority 1: A failed ZIP extraction is a total job failure.
        Optional<ZipMaster> failedZip = associatedZips.stream().filter(zip -> zip.getZipProcessingStatus() ==
                                                                              ZipProcessingStatus.EXTRACTION_FAILED)
                                                      .findFirst();
        if (failedZip.isPresent()) {
            String errorMessage = "ZIP extraction failed. Reason: " + failedZip.get().getErrorMessage();
            log.error("Job ID {} has a failed ZIP extraction. Marking job as FAILED.", jobId);
            jobLifecycleManager.failJob(jobId, errorMessage);
            return;
        }

        // Priority 2: If any component is still running, wait for the next cycle.
        if (isWorkPending(associatedZips, associatedFiles, associatedGxRecords)) {
            log.debug("Job ID {} still has active components. Will check again later.", jobId);
            return;
        }

        // Priority 3: If no work has even started, wait.
        if (associatedZips.isEmpty() && associatedFiles.isEmpty()) {
            log.debug("Job ID {} has no associated components yet. Skipping for now.", jobId);
            return;
        }

        JobStatusSummary summary = summarizeFinalStatus(associatedFiles, associatedGxRecords);


        // Case 1: There is a mix of success and failure -> PARTIAL_SUCCESS
        if (summary.successCount() > 0 && summary.failedCount() > 0) {
            String remark = createRemark(summary);
            log.warn("Job ID {} has partial failures. Marking as PARTIAL_SUCCESS with remark: {}", jobId, remark);
            jobLifecycleManager.partiallyCompleteJob(jobId, remark);
        }
        // Case 2: There are failures, but absolutely no successes -> FAILED
        else if (summary.failedCount() > 0) {
            log.error("Job ID {} has failed. No files succeeded. First error: {}", jobId, summary.firstErrorMessage());
            jobLifecycleManager.failJob(jobId, summary.firstErrorMessage());
        }
        // Case 3: There are zero failures -> COMPLETED
        // This is true even if all files were ignored or skipped, as the system itself did not error.
        else {
            String remark = createRemark(summary);
            log.info("Job ID {} has completed with zero failures. Marking as COMPLETED with remark: {}", jobId, remark);
            jobLifecycleManager.completeJob(jobId, remark);
        }
    }

    /**
     * Checks if any ZipMaster, FileMaster, or GxMaster records are still in an active, non-terminal state.
     */
    private boolean isWorkPending(List<ZipMaster> zips, List<FileMaster> files, List<GxMaster> gxRecords) {
        if (zips.stream().anyMatch(zip -> PENDING_ZIP_STATUSES.contains(zip.getZipProcessingStatus()))) return true;
        if (files.stream().anyMatch(file -> PENDING_FILE_STATUSES.contains(file.getFileProcessingStatus())))
            return true;
        return gxRecords.stream().anyMatch(gx -> PENDING_GX_STATUSES.contains(gx.getGxStatus()));
    }

    /**
     * Aggregates the final outcomes of all files associated with a job into a detailed summary.
     */
    private JobStatusSummary summarizeFinalStatus(List<FileMaster> files, List<GxMaster> gxRecords) {
        if (files.isEmpty()) {
            return new JobStatusSummary(0, 0, 0, 0, 0, "");
        }

        int successCount = 0;
        int failedCount = 0;
        int ignoredCount = 0;
        int skippedDuplicateCount = 0;
        String firstErrorMessage = "The job failed, but no specific error message was captured.";

        Map<Long, GxMaster> gxFailures = gxRecords.stream().filter(gx -> gx.getGxStatus() == GxStatus.ERROR).collect(
                Collectors.toMap(gx -> gx.getSourceFile().getId(), gx -> gx));

        for (final FileMaster file : files) {
            // Check for failures first, as they are the highest priority.
            if (file.getFileProcessingStatus() == FileProcessingStatus.FAILED) {
                failedCount++;
                if (failedCount == 1) {
                    firstErrorMessage = "File '%s' failed during processing: %s".formatted(file.getFileName(),
                                                                                           file.getErrorMessage());
                }
            } else if (gxFailures.containsKey(file.getId())) {
                GxMaster failedGx = gxFailures.get(file.getId());
                failedCount++;
                if (failedCount == 1) {
                    firstErrorMessage = "File '%s' failed during GX upload: %s".formatted(
                            failedGx.getProcessedFileName(), failedGx.getErrorMessage());
                }
            }
            // If not failed, check for other terminal states.
            else if (file.getFileProcessingStatus() == FileProcessingStatus.IGNORED) {
                ignoredCount++;
            } else if (file.getFileProcessingStatus() == FileProcessingStatus.DUPLICATE) {
                skippedDuplicateCount++;
            }
            // If it's in none of the above states, it must have completed successfully.
            else if (file.getFileProcessingStatus() == FileProcessingStatus.COMPLETED) {
                successCount++;
            }
        }
        return new JobStatusSummary(files.size(), successCount, ignoredCount, skippedDuplicateCount, failedCount,
                                    firstErrorMessage);
    }

    /**
     * Creates a human-readable summary remark for jobs with partial success.
     */
    private String createRemark(JobStatusSummary summary) {
        List<String> parts = new ArrayList<>();
        if (summary.successCount() > 0) {
            parts.add(summary.successCount() + " succeeded");
        }
        if (summary.failedCount() > 0) {
            parts.add(summary.failedCount() + " failed");
        }
        if (summary.ignoredCount() > 0) {
            parts.add(summary.ignoredCount() + " ignored");
        }
        if (summary.skippedDuplicateCount() > 0) {
            parts.add(summary.skippedDuplicateCount() + " skipped as duplicates");
        }
        return "Summary: " + String.join(", ", parts) + ".";
    }

    /**
     * A private record to hold a detailed, aggregated status summary of a processing job.
     */
    private record JobStatusSummary(int totalFileCount,
                                    int successCount,
                                    int ignoredCount,
                                    int skippedDuplicateCount,
                                    int failedCount,
                                    String firstErrorMessage) {
    }
}