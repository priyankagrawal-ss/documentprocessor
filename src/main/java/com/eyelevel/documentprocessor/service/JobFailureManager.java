package com.eyelevel.documentprocessor.service;

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
 * A centralized service for managing the state of failed jobs.
 * This service uses REQUIRES_NEW transaction propagation to ensure that failure states
 * are committed to the database immediately and independently of any calling transaction.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class JobFailureManager {

    private final ProcessingJobRepository processingJobRepository;
    private final ZipMasterRepository zipMasterRepository;
    private final FileMasterRepository fileMasterRepository;

    /**
     * Marks a ZIP-related job as FAILED in a new, independent transaction.
     *
     * @param zipMasterId  The ID of the ZipMaster that is part of the failed job.
     * @param errorMessage The detailed error message to record.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markZipJobAsFailed(Long zipMasterId, String errorMessage) {
        log.warn("Executing ZIP failure protocol for ZipMaster ID: {}", zipMasterId);
        try {
            ZipMaster zipMaster = zipMasterRepository.findById(zipMasterId).orElse(null);
            if (zipMaster == null) {
                log.error("Could not find ZipMaster with ID {} to mark as failed.", zipMasterId);
                return;
            }

            ProcessingJob job = zipMaster.getProcessingJob();
            if (job == null) {
                log.error("ZipMaster ID {} has no associated ProcessingJob to mark as failed.", zipMasterId);
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

            log.info("Successfully persisted FAILED status for Job ID {}.", job.getId());
        } catch (Exception e) {
            log.error("CRITICAL: Failed to execute the job failure protocol for ZipMaster ID: {}. The job state might be inconsistent.", zipMasterId, e);
        }
    }

    /**
     * Marks an individual file and its parent ProcessingJob as FAILED in a new, independent transaction.
     *
     * @param fileMasterId The ID of the FileMaster that failed processing.
     * @param errorMessage The detailed error message to record.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markFileJobAsFailed(Long fileMasterId, String errorMessage) {
        log.warn("Executing file failure protocol for FileMaster ID: {}", fileMasterId);
        try {
            FileMaster fileMaster = fileMasterRepository.findById(fileMasterId).orElse(null);
            if (fileMaster == null) {
                log.error("Could not find FileMaster with ID {} to mark as failed.", fileMasterId);
                return;
            }

            ProcessingJob job = fileMaster.getProcessingJob();
            if (job == null) {
                log.error("FileMaster ID {} has no associated ProcessingJob to mark as failed.", fileMasterId);
                return;
            }

            log.error("Marking Job ID {} (via FileMaster ID {}) as FAILED. Reason: {}", job.getId(), fileMasterId, errorMessage);

            fileMaster.setFileProcessingStatus(FileProcessingStatus.FAILED);
            fileMaster.setErrorMessage(errorMessage);
            fileMasterRepository.save(fileMaster);

            if (job.getStatus() != ProcessingStatus.FAILED) {
                job.setStatus(ProcessingStatus.FAILED);
                job.setCurrentStage("Individual File Processing Failed");
                job.setErrorMessage("One or more files failed processing. First failure occurred on FileMaster ID " + fileMasterId + ".");
                processingJobRepository.save(job);
            }

            log.info("Successfully persisted FAILED status for FileMaster ID {} and its parent Job ID {}.", fileMasterId, job.getId());
        } catch (Exception e) {
            log.error("CRITICAL: Failed to execute the job failure protocol for FileMaster ID: {}. The job state might be inconsistent.", fileMasterId, e);
        }
    }
}