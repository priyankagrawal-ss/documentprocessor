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
 * Orchestrates the creation and initiation of document processing jobs.
 * This service acts as the primary entry point for the API layer, handling both
 * single and bulk (ZIP) upload workflows.
 */
@Slf4j
@Service
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
     * Creates a new {@link ProcessingJob} record in the {@code PENDING_UPLOAD} state and
     * generates a pre-signed S3 URL for the client to upload a file.
     *
     * @param fileName      The original name of the file to be uploaded.
     * @param gxBucketId    (Optional) The target bucket ID. If null, the job is a bulk upload.
     * @param skipGxProcess A flag to skip the final step of sending the document to GroundX.
     * @return A {@link PresignedUploadResponse} containing the new job's ID and the upload URL.
     */
    @Transactional
    public PresignedUploadResponse createJobAndPresignedUrl(final String fileName, final Integer gxBucketId, final boolean skipGxProcess) {
        final String jobType = (gxBucketId == null) ? "BULK" : "SINGLE";
        log.info("Creating new {} processing job for file: '{}'.", jobType, fileName);

        ProcessingJob job = new ProcessingJob();
        job.setOriginalFilename(fileName);
        job.setFileLocation("PENDING_S3_KEY"); // Placeholder until ID is generated
        job.setGxBucketId(gxBucketId);
        job.setSkipGxProcess(skipGxProcess);
        job.setStatus(ProcessingStatus.PENDING_UPLOAD);
        job.setCurrentStage("Awaiting client file upload");

        job = jobRepository.saveAndFlush(job);

        final String s3Key = S3StorageService.constructS3Key(fileName, gxBucketId, job.getId(), "source");
        job.setFileLocation(s3Key);
        jobRepository.save(job);

        final URL presignedUrl = s3StorageService.generatePresignedUploadUrl(s3Key);
        log.info("Generated pre-signed URL for Job ID: {}. S3 Key: {}", job.getId(), s3Key);
        return new PresignedUploadResponse(job.getId(), presignedUrl);
    }

    /**
     * Initiates backend processing after a client confirms a file upload.
     * This method transitions the job state and routes it to the correct SQS queue
     * based on the file type (ZIP or single file).
     *
     * @param jobId The ID of the job to trigger.
     * @throws DocumentProcessingException if the job is not found or if a bulk upload is not a ZIP file.
     */
    @Transactional
    public void triggerProcessing(final Long jobId) {
        log.info("Triggering backend processing for Job ID: {}", jobId);
        final ProcessingJob job = jobRepository.findById(jobId)
                .orElseThrow(() -> new DocumentProcessingException("ProcessingJob not found with ID: " + jobId));

        job.setStatus(ProcessingStatus.UPLOAD_COMPLETE);
        final String extension = FilenameUtils.getExtension(job.getOriginalFilename()).toLowerCase();
        final boolean isZip = "zip".equals(extension);

        if (job.isBulkUpload()) {
            if (!isZip) {
                final String errorMsg = "Bulk uploads must be a ZIP file. Received: " + extension;
                log.error("Job ID {} failed validation: {}", jobId, errorMsg);
                job.setStatus(ProcessingStatus.FAILED);
                job.setErrorMessage(errorMsg);
                jobRepository.save(job);
                throw new DocumentProcessingException(errorMsg);
            }
            log.info("Job ID {} is a BULK upload. Queuing for ZIP extraction.", jobId);
            queueZipForExtraction(job);
        } else {
            if (isZip) {
                log.info("Job ID {} is a SINGLE ZIP upload for GxBucketId: {}. Queuing for extraction.", jobId, job.getGxBucketId());
                queueZipForExtraction(job);
            } else {
                log.info("Job ID {} is a SINGLE FILE upload for GxBucketId: {}. Queuing for processing.", jobId, job.getGxBucketId());
                queueFileForProcessing(job);
            }
        }

        job.setStatus(ProcessingStatus.QUEUED);
        jobRepository.save(job);
        log.info("Job ID {} has been successfully queued for processing.", jobId);
    }

    /**
     * Creates a {@link ZipMaster} record and queues it for extraction.
     */
    private void queueZipForExtraction(final ProcessingJob job) {
        job.setCurrentStage("Queued for ZIP Extraction");
        final ZipMaster zipMaster = ZipMaster.builder()
                .processingJob(job)
                .gxBucketId(job.getGxBucketId())
                .zipProcessingStatus(ZipProcessingStatus.QUEUED_FOR_EXTRACTION)
                .originalFilePath(job.getFileLocation())
                .originalFileName(job.getOriginalFilename())
                .build();
        zipMasterRepository.save(zipMaster);
        log.info("Created ZipMaster ID: {} for Job ID: {}", zipMaster.getId(), job.getId());

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                sqsTemplate.send(zipQueueName, Map.of("zipMasterId", zipMaster.getId()));
                log.info("Successfully sent ZipMaster ID: {} to queue '{}'", zipMaster.getId(), zipQueueName);
            }
        });
    }

    /**
     * Creates a {@link FileMaster} record and queues it for direct processing.
     */
    private void queueFileForProcessing(final ProcessingJob job) {
        job.setCurrentStage("Queued for File Processing");
        final FileMaster fileMaster = FileMaster.builder()
                .processingJob(job)
                .gxBucketId(job.getGxBucketId())
                .fileLocation(job.getFileLocation())
                .fileName(job.getOriginalFilename())
                .extension(FilenameUtils.getExtension(job.getOriginalFilename()).toLowerCase())
                .fileProcessingStatus(FileProcessingStatus.QUEUED)
                .sourceType(SourceType.UPLOADED)
                .build();
        fileMasterRepository.save(fileMaster);
        log.info("Created FileMaster ID: {} for Job ID: {}", fileMaster.getId(), job.getId());

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                sqsTemplate.send(fileQueueName, Map.of("fileMasterId", fileMaster.getId()));
                log.info("Successfully sent FileMaster ID: {} to queue '{}'", fileMaster.getId(), fileQueueName);
            }
        });
    }
}