package com.eyelevel.documentprocessor.service.lifecycle;

import com.eyelevel.documentprocessor.model.*;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.ProcessingJobRepository;
import com.eyelevel.documentprocessor.repository.ZipMasterRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * A centralized service for managing the final lifecycle state of processing jobs.
 * <p>
 * This service uses {@code Propagation.REQUIRES_NEW} for all its methods to ensure that
 * state changes (e.g., marking a job as FAILED or COMPLETED) are committed immediately
 * in their own independent transaction. This guarantees that the final state is durably
 * recorded, even if the calling transaction is rolled back.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class JobLifecycleManager {

    private final ProcessingJobRepository processingJobRepository;
    private final ZipMasterRepository zipMasterRepository;
    private final FileMasterRepository fileMasterRepository;

    /**
     * Marks a job as FAILED due to a terminal error during the ZIP extraction phase.
     *
     * @param zipMasterId  The ID of the ZipMaster associated with the failed job.
     * @param errorMessage The detailed error message to record.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void failJobForZipExtraction(final Long zipMasterId, final String errorMessage) {
        log.warn("Executing ZIP failure protocol for ZipMaster ID: {}", zipMasterId);
        try {
            final ZipMaster zipMaster = zipMasterRepository.findById(zipMasterId).orElse(null);
            if (zipMaster == null) {
                log.error("Cannot fail job: ZipMaster with ID {} not found.", zipMasterId);
                return;
            }

            final ProcessingJob job = zipMaster.getProcessingJob();
            if (job == null) {
                log.error("Cannot fail job: ZipMaster ID {} has no associated ProcessingJob.", zipMasterId);
                return;
            }

            log.error("Marking Job ID {} (via ZipMaster ID {}) as FAILED. Reason: {}", job.getId(), zipMasterId, errorMessage);

            zipMaster.setZipProcessingStatus(ZipProcessingStatus.EXTRACTION_FAILED);
            zipMaster.setErrorMessage(errorMessage);
            zipMasterRepository.save(zipMaster);

            job.setStatus(ProcessingStatus.FAILED);
            job.setCurrentStage("ZIP Extraction Failed");
            job.setErrorMessage(errorMessage);
            processingJobRepository.save(job);

        } catch (final Exception e) {
            log.error("CRITICAL: Failed to execute the job failure protocol for ZipMaster ID: {}. The job state may be inconsistent.", zipMasterId, e);
        }
    }

    /**
     * Marks a job as FAILED due to an error in the individual file processing pipeline.
     *
     * @param fileMasterId The ID of the FileMaster that failed processing.
     * @param errorMessage The detailed error message to record.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void failJobForFileProcessing(final Long fileMasterId, final String errorMessage) {
        log.warn("Executing file failure protocol for FileMaster ID: {}", fileMasterId);
        try {
            final FileMaster fileMaster = fileMasterRepository.findById(fileMasterId).orElse(null);
            if (fileMaster == null) {
                log.error("Cannot fail job: FileMaster with ID {} not found.", fileMasterId);
                return;
            }

            final ProcessingJob job = fileMaster.getProcessingJob();
            if (job == null) {
                log.error("Cannot fail job: FileMaster ID {} has no associated ProcessingJob.", fileMasterId);
                return;
            }

            log.error("Marking Job ID {} (via FileMaster ID {}) as FAILED. Reason: {}", job.getId(), fileMasterId, errorMessage);

            fileMaster.setFileProcessingStatus(FileProcessingStatus.FAILED);
            fileMaster.setErrorMessage(errorMessage);
            fileMasterRepository.save(fileMaster);

            if (job.getStatus() != ProcessingStatus.FAILED) {
                job.setStatus(ProcessingStatus.FAILED);
                job.setCurrentStage("Individual File Processing Failed");
                job.setErrorMessage("One or more files failed processing. First failure on FileMaster ID " + fileMasterId + ".");
                processingJobRepository.save(job);
            }
        } catch (final Exception e) {
            log.error("CRITICAL: Failed to execute the job failure protocol for FileMaster ID: {}. Job state may be inconsistent.", fileMasterId, e);
        }
    }

    /**
     * Marks a job as COMPLETED. This is typically called by the {@link com.eyelevel.documentprocessor.scheduler.JobLifecycleScheduler}.
     *
     * @param jobId The ID of the job to mark as complete.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void completeJob(final Long jobId) {
        processingJobRepository.findById(jobId).ifPresent(job -> {
            if (job.getStatus() != ProcessingStatus.COMPLETED && job.getStatus() != ProcessingStatus.FAILED) {
                job.setStatus(ProcessingStatus.COMPLETED);
                job.setCurrentStage("All files processed and uploaded successfully");
                job.setErrorMessage(null);
                processingJobRepository.save(job);
                log.info("Successfully marked Job ID {} as COMPLETED.", jobId);
            }
        });
    }

    /**
     * Marks a job as FAILED. This is typically called by the {@link com.eyelevel.documentprocessor.scheduler.JobLifecycleScheduler}
     * for errors discovered during the final status check (e.g., a GX failure).
     *
     * @param jobId  The ID of the job to mark as failed.
     * @param reason The reason for the failure.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void failJob(final Long jobId, final String reason) {
        processingJobRepository.findById(jobId).ifPresent(job -> {
            if (job.getStatus() != ProcessingStatus.FAILED) {
                job.setStatus(ProcessingStatus.FAILED);
                job.setCurrentStage("Job failed during final status check");
                job.setErrorMessage(reason);
                processingJobRepository.save(job);
                log.warn("Successfully marked Job ID {} as FAILED. Reason: {}", jobId, reason);
            }
        });
    }
}