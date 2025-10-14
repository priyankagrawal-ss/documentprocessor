package com.eyelevel.documentprocessor.service;

import com.eyelevel.documentprocessor.model.FileMaster;
import com.eyelevel.documentprocessor.model.FileProcessingStatus;
import com.eyelevel.documentprocessor.model.ProcessingJob;
import com.eyelevel.documentprocessor.model.ProcessingStatus;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.ProcessingJobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;

/**
 * A dedicated service to check for and finalize the completion of a ProcessingJob.
 * It runs in a new transaction to ensure it has a consistent view of the database.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class JobCompletionService {

    private final ProcessingJobRepository processingJobRepository;
    private final FileMasterRepository fileMasterRepository;

    private static final Set<FileProcessingStatus> FINAL_STATUSES = Set.of(
            FileProcessingStatus.COMPLETED,
            FileProcessingStatus.SKIPPED_DUPLICATE,
            FileProcessingStatus.IGNORED
    );

    /**
     * Checks if all files for a job have reached a final state. If so, marks the parent job as COMPLETED.
     * This method runs in a new, read-only transaction for the check.
     *
     * @param jobId The ID of the ProcessingJob to check.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public void checkForJobCompletion(Long jobId) {
        log.info("[Completion Check] Checking final status for Job ID: {}", jobId);

        ProcessingJob job = processingJobRepository.findById(jobId).orElse(null);
        if (job == null) {
            log.warn("[Completion Check] Job ID {} not found. Cannot check for completion.", jobId);
            return;
        }

        if (job.getStatus() == ProcessingStatus.COMPLETED || job.getStatus() == ProcessingStatus.FAILED) {
            log.debug("[Completion Check] Job ID {} is already in a terminal state ({}). Skipping check.", jobId, job.getStatus());
            return;
        }

        List<FileMaster> associatedFiles = fileMasterRepository.findAllByProcessingJobId(jobId);

        if (associatedFiles.isEmpty()) {
            log.info("[Completion Check] Job ID {} has no associated files yet. Cannot determine completion.", jobId);
            return;
        }

        boolean hasQueuedOrInProgressFiles = associatedFiles.stream()
                .anyMatch(file -> file.getFileProcessingStatus() == FileProcessingStatus.QUEUED || file.getFileProcessingStatus() == FileProcessingStatus.IN_PROGRESS);

        if (!hasQueuedOrInProgressFiles) {
            boolean allFilesAreFinal = associatedFiles.stream()
                    .allMatch(file -> FINAL_STATUSES.contains(file.getFileProcessingStatus()));

            if (allFilesAreFinal) {
                log.info("[Completion Check] All files for Job ID {} have reached a final state. Marking job as COMPLETED.", jobId);
                setJobAsCompleted(jobId);
            }
        } else {
            log.info("[Completion Check] Job ID {} still has files in progress. Completion not yet reached.", jobId);
        }
    }

    /**
     * Updates a ProcessingJob's status to COMPLETED. This runs in its own write transaction.
     *
     * @param jobId The ID of the job to update.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void setJobAsCompleted(Long jobId) {
        processingJobRepository.findById(jobId).ifPresent(job -> {
            if (job.getStatus() != ProcessingStatus.COMPLETED && job.getStatus() != ProcessingStatus.FAILED) {
                job.setStatus(ProcessingStatus.COMPLETED);
                job.setCurrentStage("All files processed successfully");
                job.setErrorMessage(null);
                processingJobRepository.save(job);
                log.info("Successfully marked Job ID {} as COMPLETED.", jobId);
            }
        });
    }
}