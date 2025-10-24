package com.eyelevel.documentprocessor.scheduler;

import com.eyelevel.documentprocessor.model.*;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.GxMasterRepository;
import com.eyelevel.documentprocessor.repository.ProcessingJobRepository;
import com.eyelevel.documentprocessor.repository.ZipMasterRepository;
import com.eyelevel.documentprocessor.service.job.JobLifecycleManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A scheduler that periodically checks the status of all non-terminal processing jobs
 * and updates them to a final state (COMPLETED, FAILED, or PARTIAL_SUCCESS).
 * This scheduler is designed to be idempotent and can re-evaluate jobs that have had
 * failed components retried by the user.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JobLifecycleScheduler {

    // MODIFIED: This set now includes failed/partial states, allowing the scheduler
    // to re-evaluate them after a retry request.
    private static final Set<ProcessingStatus> ACTIVE_AND_RETRYABLE_STATUSES = Set.of(ProcessingStatus.QUEUED,
                                                                                      ProcessingStatus.PROCESSING,
                                                                                      ProcessingStatus.UPLOAD_COMPLETE,
                                                                                      ProcessingStatus.FAILED,
                                                                                      ProcessingStatus.PARTIAL_SUCCESS);

    private static final Set<ZipProcessingStatus> PENDING_ZIP_STATUSES = Set.of(
            ZipProcessingStatus.QUEUED_FOR_EXTRACTION, ZipProcessingStatus.EXTRACTION_IN_PROGRESS);

    private static final Set<FileProcessingStatus> PENDING_FILE_STATUSES = Set.of(FileProcessingStatus.QUEUED,
                                                                                  FileProcessingStatus.IN_PROGRESS);

    // MODIFIED: Includes UPLOAD_FAILED as a pending (potentially retryable) state.
    private static final Set<GxStatus> PENDING_GX_STATUSES = Set.of(GxStatus.QUEUED_FOR_UPLOAD, GxStatus.QUEUED,
                                                                    GxStatus.PROCESSING, GxStatus.ERROR);

    private final ProcessingJobRepository processingJobRepository;
    private final ZipMasterRepository zipMasterRepository;
    private final FileMasterRepository fileMasterRepository;
    private final GxMasterRepository gxMasterRepository;
    private final JobLifecycleManager jobLifecycleManager;

    /**
     * Runs on a fixed schedule to find and finalize jobs that are not yet in a final
     * COMPLETED or TERMINATED state.
     */
    @Scheduled(cron = "${app.scheduler.job-completion-check}")
    public void finalizeJobs() {
        log.info("Starting scheduled job lifecycle finalization check...");
        final List<ProcessingJob> jobsToEvaluate = processingJobRepository.findByStatusIn(
                ACTIVE_AND_RETRYABLE_STATUSES);

        if (jobsToEvaluate.isEmpty()) {
            log.info("No active or retryable jobs found to finalize. Scheduler run is complete.");
            return;
        }

        log.info("Found {} active or retryable jobs to check for finalization.", jobsToEvaluate.size());
        for (final ProcessingJob job : jobsToEvaluate) {
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
     * This method now runs inside a transaction and uses efficient queries.
     */
    @Transactional(readOnly = true)
    protected void determineJobFinalState(final ProcessingJob job) {
        final long jobId = job.getId();

        // --- CORRECTED & EFFICIENT DATA FETCHING ---
        // This now fetches only the records related to this specific job directly from the database.
        // It is highly performant and completely avoids the LazyInitializationException.
        final List<ZipMaster> associatedZips = zipMasterRepository.findAllByProcessingJobId(jobId);
        final List<FileMaster> associatedFiles = fileMasterRepository.findAllByProcessingJobId(jobId);
        final List<GxMaster> associatedGxRecords = gxMasterRepository.findAllByProcessingJobId(jobId);

        // Priority 1: A failed ZIP extraction is a total job failure.
        Optional<ZipMaster> failedZip = associatedZips.stream()
                                                      .filter(zip -> zip.getZipProcessingStatus() == ZipProcessingStatus.EXTRACTION_FAILED)
                                                      .findFirst();
        if (failedZip.isPresent()) {
            String errorMessage = "ZIP extraction failed. Reason: " + failedZip.get().getErrorMessage();
            jobLifecycleManager.failJob(jobId, errorMessage);
            return;
        }

        // Priority 2: If any component is still running, the job is not yet final. Wait.
        if (isWorkPending(associatedZips, associatedFiles, associatedGxRecords)) {
            log.debug("Job ID {} still has active components. Will check again later.", jobId);
            return;
        }

        // Priority 3: If no work has started yet (e.g., job is QUEUED), wait.
        if (associatedZips.isEmpty() && associatedFiles.isEmpty()) {
            log.debug("Job ID {} has no associated components yet. Skipping for now.", jobId);
            return;
        }

        JobStatusSummary summary = summarizeFinalStatus(associatedFiles, associatedGxRecords);

        // Apply final job status based on the summary of its children.
        if (summary.successCount() > 0 && summary.failedCount() > 0) {
            jobLifecycleManager.partiallyCompleteJob(jobId, createRemark(summary));
        } else if (summary.failedCount() > 0) {
            jobLifecycleManager.failJob(jobId, summary.firstErrorMessage());
        } else {
            jobLifecycleManager.completeJob(jobId, createRemark(summary));
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
                if (failedCount == 1) { // Capture the very first error message
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
            // If it's in none of the above states and not pending, it must have completed successfully.
            else if (file.getFileProcessingStatus() == FileProcessingStatus.COMPLETED) {
                successCount++;
            }
        }
        return new JobStatusSummary(files.size(), successCount, ignoredCount, skippedDuplicateCount, failedCount,
                                    firstErrorMessage);
    }

    /**
     * Creates a human-readable summary remark for the job.
     */
    private String createRemark(JobStatusSummary summary) {
        if (summary.totalFileCount() == 0) {
            return "Job completed, but no processable files were produced.";
        }
        List<String> parts = getParts(summary);
        if (parts.isEmpty()) {
            return "Job finished processing " + summary.totalFileCount() +
                   " files with no definitive success or failure.";
        }
        return "Summary: " + String.join(", ", parts) + ".";
    }

    private static List<String> getParts(JobStatusSummary summary) {
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
        return parts;
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