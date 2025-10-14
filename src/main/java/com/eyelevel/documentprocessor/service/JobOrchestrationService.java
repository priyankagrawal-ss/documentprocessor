package com.eyelevel.documentprocessor.service;

import com.eyelevel.documentprocessor.dto.PresignedUploadResponse;
import com.eyelevel.documentprocessor.exception.DocumentProcessingException;
import com.eyelevel.documentprocessor.model.*;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.ProcessingJobRepository;
import com.eyelevel.documentprocessor.repository.ZipMasterRepository;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.net.URL;
import java.util.Map;

/**
 * Service responsible for orchestrating the creation and initiation of document processing jobs.
 * It handles both single and bulk upload workflows.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class JobOrchestrationService {

    private final ProcessingJobRepository jobRepository;
    private final ZipMasterRepository zipMasterRepository;
    private final FileMasterRepository fileMasterRepository;
    private final SqsTemplate sqsTemplate;
    private final S3StorageService s3StorageService;

    @Value("${aws.sqs.zip-queue-name}")
    private String zipQueueName;

    @Value("${aws.sqs.file-queue-name}")
    private String fileQueueName;

    /**
     * Creates a ProcessingJob entity and generates a pre-signed S3 URL for file upload.
     *
     * @return A response object containing the new job's ID and the URL.
     */
    @Transactional
    public PresignedUploadResponse createJobAndPresignedUrl(String fileName, Integer gxBucketId, boolean skipGxProcess) {
        String jobType = (gxBucketId == null) ? "BULK" : "SINGLE";
        log.info("Creating new {} ProcessingJob for fileName: {}.", jobType, fileName);

        ProcessingJob job = new ProcessingJob();
        job.setOriginalFilename(fileName);
        job.setFileLocation("PENDING_S3_KEY");
        job.setGxBucketId(gxBucketId);
        job.setSkipGxProcess(skipGxProcess);
        job.setStatus(ProcessingStatus.PENDING_UPLOAD);
        job.setCurrentStage("Waiting for client to upload file and trigger processing");

        job = jobRepository.saveAndFlush(job);
        log.debug("Saved initial ProcessingJob with ID: {}", job.getId());

        String s3Key = S3StorageService.constructS3Key(fileName, gxBucketId, job.getId(), "source");
        job.setFileLocation(s3Key);
        jobRepository.save(job);

        URL presignedUrl = s3StorageService.generatePresignedUploadUrl(s3Key);
        log.info("Generated pre-signed URL for Job ID: {}. S3 Key: {}", job.getId(), s3Key);
        return new PresignedUploadResponse(job.getId(), presignedUrl);
    }

    /**
     * Initiates processing after a client confirms a file upload, routing to the correct queue.
     *
     * @param jobId The ID of the job to trigger.
     */
    @Transactional
    public void triggerProcessing(Long jobId) {
        log.info("Triggering processing for Job ID: {}", jobId);
        ProcessingJob job = jobRepository.findById(jobId)
                .orElseThrow(() -> {
                    log.error("Attempted to trigger processing for non-existent Job ID: {}", jobId);
                    return new DocumentProcessingException("ProcessingJob not found with ID: " + jobId);
                });

        job.setStatus(ProcessingStatus.UPLOAD_COMPLETE);
        String extension = FilenameUtils.getExtension(job.getOriginalFilename()).toLowerCase();
        boolean isZip = "zip".equals(extension);

        if (job.isBulkUpload()) {
            if (!isZip) {
                String errorMsg = "Bulk uploads must be a ZIP file. Received file type: " + extension;
                log.error("Job ID: {} is a bulk upload but the file is not a zip. Marking as FAILED.", jobId);
                job.setStatus(ProcessingStatus.FAILED);
                job.setErrorMessage(errorMsg);
                jobRepository.save(job);
                throw new DocumentProcessingException(errorMsg);
            }
            log.info("[JobId: {}] Handling as a BULK ZIP upload.", jobId);
            queueZipForExtraction(job);
        } else {
            if (isZip) {
                log.info("[JobId: {}] Handling as a SINGLE ZIP upload for GxBucketId: {}.", jobId, job.getGxBucketId());
                queueZipForExtraction(job);
            } else {
                log.info("[JobId: {}] Handling as a SINGLE FILE upload for GxBucketId: {}.", jobId, job.getGxBucketId());
                queueFileForProcessing(job);
            }
        }

        job.setStatus(ProcessingStatus.QUEUED);
        jobRepository.save(job);
        log.info("ProcessingJob ID: {} has been successfully queued.", jobId);
    }

    /**
     * Helper method to create a ZipMaster record and queue it for extraction.
     */
    private void queueZipForExtraction(ProcessingJob job) {
        job.setCurrentStage("Queued for ZIP Extraction");
        ZipMaster zipMaster = ZipMaster.builder()
                .processingJob(job)
                .gxBucketId(job.getGxBucketId())
                .zipProcessingStatus(ZipProcessingStatus.QUEUED_FOR_EXTRACTION)
                .originalFilePath(job.getFileLocation())
                .originalFileName(job.getOriginalFilename()).build();
        zipMasterRepository.save(zipMaster);
        log.info("Created ZipMaster ID: {} for Job ID: {}", zipMaster.getId(), job.getId());

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                sqsTemplate.send(zipQueueName, Map.of("zipMasterId", zipMaster.getId()));
                log.info("Successfully queued ZipMaster ID: {} to queue '{}'", zipMaster.getId(), zipQueueName);
            }
        });
    }

    /**
     * Helper method to create a FileMaster record and queue it for direct processing.
     */
    private void queueFileForProcessing(ProcessingJob job) {
        job.setCurrentStage("Queued for File Conversion");
        FileMaster fileMaster = FileMaster.builder()
                .processingJob(job)
                .gxBucketId(job.getGxBucketId())
                .fileLocation(job.getFileLocation())
                .fileName(job.getOriginalFilename())
                .extension(FilenameUtils.getExtension(job.getOriginalFilename()).toLowerCase())
                .fileProcessingStatus(FileProcessingStatus.QUEUED)
                .build();
        fileMasterRepository.save(fileMaster);
        log.info("Created FileMaster ID: {} for Job ID: {}", fileMaster.getId(), job.getId());

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                sqsTemplate.send(fileQueueName, Map.of("fileMasterId", fileMaster.getId()));
                log.info("Successfully queued FileMaster ID: {} to queue '{}'", fileMaster.getId(), fileQueueName);
            }
        });
    }
}