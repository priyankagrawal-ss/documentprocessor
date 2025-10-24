package com.eyelevel.documentprocessor.service.job;

import com.eyelevel.documentprocessor.dto.uploadfile.direct.PresignedUploadResponse;
import com.eyelevel.documentprocessor.dto.uploadfile.multipart.InitiateMultipartUploadResponse;
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
import software.amazon.awssdk.services.s3.model.CompletedPart;

import java.net.URL;
import java.util.List;
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

    @Transactional
    public PresignedUploadResponse createJobAndPresignedUrl(final String fileName, final Integer gxBucketId,
                                                            final boolean skipGxProcess) {
        ProcessingJob job = createAndPersistProcessingJob(fileName, gxBucketId, skipGxProcess,
                                                          "Awaiting client file upload");
        final URL presignedUrl = s3StorageService.generatePresignedUploadUrl(job.getFileLocation());
        log.info("Generated pre-signed URL for Job ID: {}. S3 Key: {}", job.getId(), job.getFileLocation());
        return new PresignedUploadResponse(job.getId(), presignedUrl);
    }

    @Transactional
    public InitiateMultipartUploadResponse createJobAndInitiateMultipartUpload(final String fileName,
                                                                               final Integer gxBucketId,
                                                                               final boolean skipGxProcess) {
        ProcessingJob job = createAndPersistProcessingJob(fileName, gxBucketId, skipGxProcess,
                                                          "Awaiting client file upload (multipart)");
        final String uploadId = s3StorageService.initiateMultipartUpload(job.getFileLocation());
        log.info("Initiated multipart upload for Job ID: {}. S3 Key: {}, Upload ID: {}", job.getId(),
                 job.getFileLocation(), uploadId);
        return new InitiateMultipartUploadResponse(job.getId(), uploadId);
    }

    public URL generatePresignedUrlForPart(final Long jobId, final String uploadId, final int partNumber) {
        final ProcessingJob job = findJobById(jobId);
        return s3StorageService.generatePresignedUrlForPart(job.getFileLocation(), uploadId, partNumber);
    }

    public void completeMultipartUpload(final Long jobId, final String uploadId, final List<CompletedPart> parts) {
        final ProcessingJob job = findJobById(jobId);
        s3StorageService.completeMultipartUpload(job.getFileLocation(), uploadId, parts);
        log.info("Client confirmed completion of multipart upload for Job ID: {}", jobId);
    }

    @Transactional
    public void triggerProcessing(final Long jobId) {
        log.info("Triggering backend processing for Job ID: {}", jobId);
        final ProcessingJob job = findJobById(jobId);

        if (!VALID_TRIGGER_STATUSES.contains(job.getStatus())) {
            throw new DocumentProcessingException(String.format(
                    "Job with ID %d cannot be triggered because it is already in progress or completed. Current status: %s",
                    jobId, job.getStatus()));
        }

        job.setStatus(ProcessingStatus.UPLOAD_COMPLETE);
        final String extension = FilenameUtils.getExtension(job.getOriginalFilename()).toLowerCase();
        final boolean isZip = "zip".equals(extension);

        // Guard clause: Fail fast for invalid bulk uploads
        if (job.isBulkUpload() && !isZip) {
            final String errorMsg = "Bulk uploads must be a ZIP file but received: " + extension;
            job.setStatus(ProcessingStatus.FAILED);
            job.setErrorMessage(errorMsg);
            throw new DocumentProcessingException(errorMsg);
        }

        if (job.isBulkUpload() || isZip) {
            log.info("Job ID {} is a ZIP upload. Routing to ZIP ingestion.", jobId);
            queueZipForIngestion(job);
        } else {
            log.info("Job ID {} is a SINGLE FILE upload. Routing to file processing.", jobId);
            queueFileForProcessing(job);
        }

        job.setStatus(ProcessingStatus.QUEUED);
        log.info("Job ID {} has been successfully queued for processing.", jobId);
    }

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
            log.warn("ZipMaster for Job ID {} already exists in a non-queued state ({}). Skipping queue message.",
                     job.getId(), zipMaster.getZipProcessingStatus());
            return;
        }

        log.info("Queueing ZipMaster ID: {} for Job ID: {}", zipMaster.getId(), job.getId());
        queueSqsMessageAfterCommit(zipQueueName, Map.of("zipMasterId", zipMaster.getId()), "zip-job-" + job.getId(),
                                   "zip-master-" + zipMaster.getId());
    }

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
            log.warn("FileMaster for Job ID {} already exists in a non-queued state ({}). Skipping queue message.",
                     job.getId(), fileMaster.getFileProcessingStatus());
            return;
        }

        log.info("Queueing FileMaster ID: {} for Job ID: {}", fileMaster.getId(), job.getId());
        queueSqsMessageAfterCommit(fileQueueName, Map.of("fileMasterId", fileMaster.getId()),
                                   String.valueOf(fileMaster.getGxBucketId()),
                                   "file-master-" + fileMaster.getId() + "-" + UUID.randomUUID());
    }

    /**
     * Centralizes the creation of a {@link ProcessingJob} to avoid code duplication.
     *
     * @return The persisted {@link ProcessingJob} entity.
     */
    private ProcessingJob createAndPersistProcessingJob(final String fileName, final Integer gxBucketId,
                                                        final boolean skipGxProcess, final String initialStage) {
        final String jobType = (gxBucketId == null) ? "BULK" : "SINGLE";
        log.info("Creating new {} processing job for file: '{}', skipGxProcess={}", jobType, fileName, skipGxProcess);

        ProcessingJob job = new ProcessingJob();
        job.setOriginalFilename(fileName);
        job.setGxBucketId(gxBucketId);
        job.setSkipGxProcess(skipGxProcess);
        job.setStatus(ProcessingStatus.PENDING_UPLOAD);
        job.setCurrentStage(initialStage);
        // Save first to generate an ID
        job.setFileLocation("PENDING");
        job = jobRepository.saveAndFlush(job);

        // Now construct the S3 key with the generated ID and update the job
        final String s3Key = S3StorageService.constructS3Key(fileName, gxBucketId, job.getId(), "source");
        job.setFileLocation(s3Key);
        return jobRepository.save(job);
    }

    /**
     * Centralizes the logic for finding a job by its ID, throwing a consistent exception if not found.
     *
     * @param jobId The ID of the job to find.
     *
     * @return The {@link ProcessingJob} entity.
     */
    private ProcessingJob findJobById(final Long jobId) {
        return jobRepository.findById(jobId).orElseThrow(
                () -> new DocumentProcessingException("ProcessingJob not found with ID: " + jobId));
    }

    /**
     * Encapsulates the logic to send an SQS message only after the current database transaction commits successfully.
     */
    private void queueSqsMessageAfterCommit(final String queueName, final Map<String, ?> payload, final String groupId,
                                            final String deduplicationId) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                sqsTemplate.send(to -> to.queue(queueName).payload(payload).header(SQS_MESSAGE_GROUP_ID_HEADER, groupId)
                                         .header(SQS_MESSAGE_DEDUPLICATION_ID_HEADER, deduplicationId));
                log.info("Successfully sent message to queue '{}' with payload: {}", queueName, payload);
            }
        });
    }
}