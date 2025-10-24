package com.eyelevel.documentprocessor.service.job;

import com.eyelevel.documentprocessor.model.*;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.GxMasterRepository;
import com.eyelevel.documentprocessor.repository.ProcessingJobRepository;
import com.eyelevel.documentprocessor.repository.ZipMasterRepository;
import com.eyelevel.documentprocessor.service.sqs.SqsManagerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Set;

/**
 * A centralized service for managing the final lifecycle state of processing jobs.
 * This service uses {@code Propagation.REQUIRES_NEW} for its single-job methods
 * to ensure state changes are committed immediately and atomically.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class JobLifecycleManager {

    private final ProcessingJobRepository processingJobRepository;
    private final ZipMasterRepository zipMasterRepository;
    private final FileMasterRepository fileMasterRepository;
    private final GxMasterRepository gxMasterRepository;
    private final SqsManagerService sqsManagerService;
    private JobLifecycleManager self;

    private static final Set<ProcessingStatus> TERMINABLE_JOB_STATUSES = Set.of(
            ProcessingStatus.PENDING_UPLOAD,
            ProcessingStatus.UPLOAD_COMPLETE,
            ProcessingStatus.QUEUED,
            ProcessingStatus.PROCESSING
    );

    @Autowired
    public void setSelf(@Lazy JobLifecycleManager self) {
        this.self = self;
    }

    /**
     * OPTIMIZED: Terminates all active jobs and purges queues in a single, efficient transaction.
     * This method avoids the N+1 transaction problem by using bulk update operations.
     *
     * @return The number of jobs that were marked for termination.
     */
    @Transactional
    public int terminateAllActiveJobs() {
        log.warn("ADMIN ACTION: Initiating termination of ALL active jobs using an optimized bulk operation.");

        final List<ProcessingJob> activeJobs = processingJobRepository.findByStatusIn(TERMINABLE_JOB_STATUSES);

        if (CollectionUtils.isEmpty(activeJobs)) {
            log.info("ADMIN ACTION: No active jobs found to terminate.");
            sqsManagerService.purgeAllQueues(); // Purge queues even if no jobs are active to clear any stragglers.
            return 0;
        }

        final List<Long> jobIdsToTerminate = activeJobs.stream().map(ProcessingJob::getId).toList();
        int count = jobIdsToTerminate.size();
        log.info("Found {} active jobs to terminate. Proceeding with bulk update.", count);

        // --- Execute Bulk Updates ---
        // 1. Terminate parent ProcessingJob records.
        processingJobRepository.updateStatusForIds(jobIdsToTerminate, ProcessingStatus.TERMINATED, "Job terminated by bulk admin action");

        // 2. Terminate associated ZipMaster records.
        final List<ZipProcessingStatus> terminableZipStatuses = List.of(ZipProcessingStatus.QUEUED_FOR_EXTRACTION, ZipProcessingStatus.EXTRACTION_IN_PROGRESS);
        zipMasterRepository.updateStatusForJobIds(jobIdsToTerminate, ZipProcessingStatus.TERMINATED, terminableZipStatuses);

        // 3. Terminate associated FileMaster records.
        final List<FileProcessingStatus> terminableFileStatuses = List.of(FileProcessingStatus.QUEUED, FileProcessingStatus.IN_PROGRESS);
        fileMasterRepository.updateStatusForJobIds(jobIdsToTerminate, FileProcessingStatus.TERMINATED, terminableFileStatuses);

        // 4. Terminate associated GxMaster records to prevent them from being picked up by schedulers.
        final List<GxStatus> terminableGxStatuses = List.of(GxStatus.QUEUED_FOR_UPLOAD);
        gxMasterRepository.updateStatusForJobIds(jobIdsToTerminate, GxStatus.TERMINATED, terminableGxStatuses);

        // After all database updates are part of this single transaction, purge the SQS queues.
        sqsManagerService.purgeAllQueues();

        log.warn("ADMIN ACTION: Completed termination request for {} active jobs and purged queues.", count);
        return count;
    }

    /**
     * Terminates a single job and its children in a new, independent transaction.
     * This is called for individual termination requests from the API.
     *
     * @param jobId The ID of the job to terminate.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void terminateJob(final Long jobId) {
        log.warn("Executing termination protocol for Job ID: {}", jobId);
        processingJobRepository.findById(jobId).ifPresent(job -> {
            if (!TERMINABLE_JOB_STATUSES.contains(job.getStatus())) {
                log.warn("Job ID {} is already in a final state ({}) and cannot be terminated.", jobId, job.getStatus());
                return;
            }

            job.setStatus(ProcessingStatus.TERMINATED);
            job.setCurrentStage("Job terminated by user request");
            processingJobRepository.save(job);
            log.info("Successfully marked Job ID {} as TERMINATED.", jobId);

            // Use efficient bulk updates even for a single job ID.
            List<Long> jobIdList = List.of(jobId);

            final List<ZipProcessingStatus> terminableZipStatuses = List.of(ZipProcessingStatus.QUEUED_FOR_EXTRACTION, ZipProcessingStatus.EXTRACTION_IN_PROGRESS);
            zipMasterRepository.updateStatusForJobIds(jobIdList, ZipProcessingStatus.TERMINATED, terminableZipStatuses);

            final List<FileProcessingStatus> terminableFileStatuses = List.of(FileProcessingStatus.QUEUED, FileProcessingStatus.IN_PROGRESS);
            fileMasterRepository.updateStatusForJobIds(jobIdList, FileProcessingStatus.TERMINATED, terminableFileStatuses);

            final List<GxStatus> terminableGxStatuses = List.of(GxStatus.QUEUED_FOR_UPLOAD);
            gxMasterRepository.updateStatusForJobIds(jobIdList, GxStatus.TERMINATED, terminableGxStatuses);
        });
    }

    // ... The rest of the methods below are already well-optimized for their specific use cases ...
    // ... No changes are needed for them. ...

    /**
     * Marks a job as FAILED due to a terminal error during the ZIP extraction phase.
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

            log.error("Marking Job ID {} (via ZipMaster ID {}) as FAILED. Reason: {}", job.getId(), zipMasterId,
                    errorMessage);

            zipMaster.setZipProcessingStatus(ZipProcessingStatus.EXTRACTION_FAILED);
            zipMaster.setErrorMessage(errorMessage);
            zipMasterRepository.save(zipMaster);

            job.setStatus(ProcessingStatus.FAILED);
            job.setCurrentStage("ZIP Extraction Failed");
            job.setErrorMessage(errorMessage);
            processingJobRepository.save(job);

        } catch (final Exception e) {
            log.error(
                    "CRITICAL: Failed to execute the job failure protocol for ZipMaster ID: {}. The job state may be inconsistent.",
                    zipMasterId, e);
        }
    }

    /**
     * Marks a job as FAILED due to an error in the individual file processing pipeline.
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

            log.error("Marking Job ID {} (via FileMaster ID {}) as FAILED. Reason: {}", job.getId(), fileMasterId,
                    errorMessage);

            fileMaster.setFileProcessingStatus(FileProcessingStatus.FAILED);
            fileMaster.setErrorMessage(errorMessage);
            fileMasterRepository.save(fileMaster);

            if (job.getStatus() != ProcessingStatus.FAILED) {
                job.setStatus(ProcessingStatus.FAILED);
                job.setCurrentStage("Individual File Processing Failed");
                job.setErrorMessage(
                        "One or more files failed processing. First failure on FileMaster ID " + fileMasterId + ".");
                processingJobRepository.save(job);
            }
        } catch (final Exception e) {
            log.error(
                    "CRITICAL: Failed to execute the job failure protocol for FileMaster ID: {}. Job state may be inconsistent.",
                    fileMasterId, e);
        }
    }

    /**
     * Marks a job as COMPLETED.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void completeJob(final Long jobId, String remark) {
        processingJobRepository.findById(jobId).ifPresent(job -> {
            if (job.getStatus() != ProcessingStatus.COMPLETED && job.getStatus() != ProcessingStatus.FAILED) {
                job.setStatus(ProcessingStatus.COMPLETED);
                job.setCurrentStage("All files processed and uploaded successfully");
                job.setErrorMessage(null);
                job.setRemark(remark);
                processingJobRepository.save(job);
                log.info("Successfully marked Job ID {} as COMPLETED.", jobId);
            }
        });
    }

    /**
     * Marks a job as PARTIALLY_SUCCESSFUL.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void partiallyCompleteJob(final Long jobId, final String remark) {
        processingJobRepository.findById(jobId).ifPresent(job -> {
            if (job.getStatus() != ProcessingStatus.COMPLETED && job.getStatus() != ProcessingStatus.FAILED) {
                job.setStatus(ProcessingStatus.PARTIAL_SUCCESS);
                job.setCurrentStage("Job completed with some file failures");
                job.setRemark(remark);
                job.setErrorMessage(null); // Clear any transient error messages
                processingJobRepository.save(job);
                log.info("Successfully marked Job ID {} as PARTIAL_SUCCESS. Remark: {}", jobId, remark);
            }
        });
    }

    /**
     * Marks a job as FAILED.
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

    /**
     * NEW METHOD: Marks a GxMaster as FAILED in a new, independent transaction.
     * This is called when an asynchronous S3 upload for a final artifact fails.
     *
     * @param gxMasterId   The ID of the GxMaster that failed to upload.
     * @param errorMessage The detailed error message.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void failGxMasterUpload(final Long gxMasterId, final String errorMessage) {
        log.warn("Executing GX Master failure protocol for GxMaster ID: {}", gxMasterId);
        gxMasterRepository.findById(gxMasterId).ifPresent(gxMaster -> {
            if (gxMaster.getGxStatus() != GxStatus.ERROR) {
                gxMaster.setGxStatus(GxStatus.ERROR);
                gxMaster.setErrorMessage(errorMessage);
                gxMasterRepository.save(gxMaster);
                log.error("Marking GxMaster ID {} as ERROR. Reason: {}", gxMasterId, errorMessage);
            }
        });
    }
}