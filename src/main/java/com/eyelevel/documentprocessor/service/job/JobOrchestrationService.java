package com.eyelevel.documentprocessor.service.job;

import com.eyelevel.documentprocessor.dto.PresignedUploadResponse;
import com.eyelevel.documentprocessor.exception.DocumentProcessingException;
import com.eyelevel.documentprocessor.model.*;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.ProcessingJobRepository;
import com.eyelevel.documentprocessor.repository.ZipMasterRepository;
import com.eyelevel.documentprocessor.service.s3.S3StorageService;
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
import java.util.Set;
import java.util.UUID;

/**
 * Orchestrates the creation and initiation of document processing jobs. This service acts as the
 * primary entry point for the API layer, handling both single and bulk (ZIP) upload workflows.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class JobOrchestrationService {

    private static final Set<ProcessingStatus> VALID_TRIGGER_STATUSES = Set.of(ProcessingStatus.PENDING_UPLOAD,
                                                                               ProcessingStatus.UPLOAD_COMPLETE);
    private static final String SQS_MESSAGE_GROUP_ID_HEADER = "message-group-id";
    private static final String SQS_MESSAGE_DEDUPLICATION_ID_HEADER = "message-deduplication-id";
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
     * Creates a new {@link ProcessingJob}, reserves an S3 key, and generates a pre-signed S3 URL
     * for the client to upload a file directly to storage.
     *
     * @param fileName      The original name of the file to be uploaded.
     * @param gxBucketId    The target bucket ID. If null, the job is treated as a bulk ZIP upload.
     * @param skipGxProcess A flag to bypass the final step of sending the document to GroundX.
     *
     * @return A {@link PresignedUploadResponse} containing the new job's ID and the upload URL.
     */
    @Transactional
    public PresignedUploadResponse createJobAndPresignedUrl(final String fileName, final Integer gxBucketId,
                                                            final boolean skipGxProcess) {
        final String jobType = (gxBucketId == null) ? "BULK" : "SINGLE";
        log.info("Creating new {} processing job for file: '{}', skipGxProcess={}", jobType, fileName, skipGxProcess);

        ProcessingJob job = new ProcessingJob();
        job.setOriginalFilename(fileName);
        job.setFileLocation("PENDING_S3_KEY");
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
     * Initiates backend processing after a client confirms a successful file upload. This method
     * transitions the job state and routes it to the correct SQS queue based on its type.
     *
     * @param jobId The ID of the job to trigger.
     *
     * @throws DocumentProcessingException if the job is not found or fails validation.
     */
    @Transactional
    public void triggerProcessing(final Long jobId) {
        log.info("Triggering backend processing for Job ID: {}", jobId);
        final ProcessingJob job = jobRepository.findById(jobId).orElseThrow(
                () -> new DocumentProcessingException("ProcessingJob not found with ID: " + jobId));

        if (!VALID_TRIGGER_STATUSES.contains(job.getStatus())) {
            String errorMessage = String.format(
                    "Job with ID %d cannot be triggered because it is already in progress or completed. Current status: %s",
                    jobId, job.getStatus());
            log.warn(errorMessage);
            throw new DocumentProcessingException(errorMessage);
        }

        job.setStatus(ProcessingStatus.UPLOAD_COMPLETE);
        final String extension = FilenameUtils.getExtension(job.getOriginalFilename()).toLowerCase();
        final boolean isZip = "zip".equals(extension);

        if (job.isBulkUpload() || isZip) {
            if (job.isBulkUpload() && !isZip) {
                final String errorMsg = "Bulk uploads must be a ZIP file but received: " + extension;
                job.setStatus(ProcessingStatus.FAILED);
                job.setErrorMessage(errorMsg);
                throw new DocumentProcessingException(errorMsg);
            }
            log.info("Job ID {} is a ZIP upload. Routing to ZIP ingestion.", jobId);
            queueZipForIngestion(job);
        } else {
            log.info("Job ID {} is a SINGLE FILE upload. Routing to file processing.", jobId);
            queueFileForProcessing(job);
        }

        job.setStatus(ProcessingStatus.QUEUED);
    }

    /**
     * Finds an existing ZipMaster or creates a new one and queues it for ingestion.
     * This method is now idempotent, optimized, and significantly cleaner.
     */
    private void queueZipForIngestion(final ProcessingJob job) {
        job.setCurrentStage("Queued for ZIP Ingestion");

        final ZipMaster zipMaster = zipMasterRepository.findByProcessingJobId(job.getId()).orElseGet(() -> {
            log.info("No existing ZipMaster found for Job ID {}. Creating a new one.", job.getId());
            ZipMaster newZipMaster = ZipMaster.builder().processingJob(job).gxBucketId(job.getGxBucketId())
                                              .zipProcessingStatus(ZipProcessingStatus.QUEUED_FOR_EXTRACTION)
                                              .originalFilePath(job.getFileLocation())
                                              .originalFileName(job.getOriginalFilename()).build();
            return zipMasterRepository.save(newZipMaster);
        });

        if (zipMaster.getZipProcessingStatus() != ZipProcessingStatus.QUEUED_FOR_EXTRACTION) {
            log.warn(
                    "ZipMaster for Job ID {} already exists and is in a non-queued state ({}). Skipping queue message.",
                    job.getId(), zipMaster.getZipProcessingStatus());
            return;
        }

        log.info("Queueing ZipMaster ID: {} for Job ID: {}", zipMaster.getId(), job.getId());
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                String messageGroupId = "zip-job-" + job.getId();
                String deduplicationId = "zip-master-" + zipMaster.getId();

                sqsTemplate.send(to -> to.queue(zipQueueName).payload(Map.of("zipMasterId", zipMaster.getId()))
                                         .header(SQS_MESSAGE_GROUP_ID_HEADER, messageGroupId)
                                         .header(SQS_MESSAGE_DEDUPLICATION_ID_HEADER, deduplicationId));
                log.info("Sent ZipMaster ID: {} to queue '{}'", zipMaster.getId(), zipQueueName);
            }
        });
    }

    /**
     * Finds an existing FileMaster or creates a new one and queues it for processing.
     * This method is now idempotent, optimized, and significantly cleaner.
     */
    private void queueFileForProcessing(final ProcessingJob job) {
        job.setCurrentStage("Queued for File Processing");

        final FileMaster fileMaster = fileMasterRepository.findByProcessingJobId(job.getId()).orElseGet(() -> {
            log.info("No existing FileMaster found for Job ID {}. Creating a new one.", job.getId());
            FileMaster newFileMaster = FileMaster.builder().processingJob(job).gxBucketId(job.getGxBucketId())
                                                 .fileLocation(job.getFileLocation())
                                                 .fileName(job.getOriginalFilename()).extension(
                            FilenameUtils.getExtension(job.getOriginalFilename()).toLowerCase())
                                                 .fileProcessingStatus(FileProcessingStatus.QUEUED)
                                                 .sourceType(SourceType.UPLOADED).build();
            return fileMasterRepository.save(newFileMaster);
        });

        if (fileMaster.getFileProcessingStatus() != FileProcessingStatus.QUEUED) {
            log.warn(
                    "FileMaster for Job ID {} already exists and is in a non-queued state ({}). Skipping queue message.",
                    job.getId(), fileMaster.getFileProcessingStatus());
            return;
        }

        log.info("Queueing FileMaster ID: {} for Job ID: {}", fileMaster.getId(), job.getId());
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                String messageGroupId = String.valueOf(fileMaster.getGxBucketId());
                String deduplicationId = "file-master-" + fileMaster.getId() + "-" + UUID.randomUUID();

                sqsTemplate.send(to -> to.queue(fileQueueName).payload(Map.of("fileMasterId", fileMaster.getId()))
                                         .header(SQS_MESSAGE_GROUP_ID_HEADER, messageGroupId)
                                         .header(SQS_MESSAGE_DEDUPLICATION_ID_HEADER, deduplicationId));
                log.info("Sent FileMaster ID: {} to queue '{}'", fileMaster.getId(), fileQueueName);
            }
        });
    }
}