package com.eyelevel.documentprocessor.service.file;

import com.eyelevel.documentprocessor.dto.retry.request.RetryRequest;
import com.eyelevel.documentprocessor.exception.RetryFailedException;
import com.eyelevel.documentprocessor.model.*;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.GxMasterRepository;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.UUID;

/**
 * Handles the logic for retrying failed processing tasks for both FileMaster and GxMaster.
 * This version correctly handles partial failures and decouples GX retries from the parent job status.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RetryService {

    private final FileMasterRepository fileMasterRepository;
    private final GxMasterRepository gxMasterRepository;
    private final SqsTemplate sqsTemplate;

    @Value("${aws.sqs.file-queue-name}")
    private String fileQueueName;

    private static final String SQS_MESSAGE_GROUP_ID_HEADER = "message-group-id";
    private static final String SQS_MESSAGE_DEDUPLICATION_ID_HEADER = "message-deduplication-id";

    /**
     * Main entry point for retry requests. Delegates to the appropriate handler.
     *
     * @param request The retry request containing either a fileMasterId or gxMasterId.
     */
    @Transactional
    public void retryFailedProcess(final RetryRequest request) {
        if (request.getFileMasterId() != null && request.getGxMasterId() == null) {
            retryFileMaster(request.getFileMasterId());
        } else if (request.getGxMasterId() != null && request.getFileMasterId() != null) {
            retryGxMaster(request.getGxMasterId());
        } else {
            throw new IllegalArgumentException("A valid fileMasterId or gxMasterId must be provided.");
        }
    }

    /**
     * Retries a failed FileMaster process.
     * This method resets the status of the FileMaster and re-queues it for processing.
     * It deliberately does NOT change the parent job's status, leaving that determination
     * to the final job lifecycle scheduler.
     *
     * @param fileMasterId The ID of the FileMaster to retry.
     */
    private void retryFileMaster(final Long fileMasterId) {
        log.info("Attempting to retry FileMaster ID: {}", fileMasterId);
        FileMaster fileMaster = fileMasterRepository.findById(fileMasterId).orElseThrow(
                () -> new RetryFailedException("FileMaster with ID " + fileMasterId + " not found."));

        if (fileMaster.getFileProcessingStatus() != FileProcessingStatus.FAILED) {
            throw new RetryFailedException("Cannot retry: FileMaster is not in a FAILED state. Current state: " +
                                           fileMaster.getFileProcessingStatus());
        }

        ProcessingJob job = fileMaster.getProcessingJob();
        validateParentJobIsRetryable(job);

        // Reset the file's status and clear its error.
        fileMaster.setFileProcessingStatus(FileProcessingStatus.QUEUED);
        fileMaster.setErrorMessage(null);
        fileMasterRepository.save(fileMaster);

        // Re-queue the file for processing.
        requeueFileMaster(fileMaster);
        log.info(
                "Successfully re-queued FileMaster ID {} for processing. The parent Job ID {} status remains unchanged and will be re-evaluated by the scheduler.",
                fileMasterId, job.getId());
    }

    /**
     * Retries a failed GxMaster upload process.
     * This method is decoupled from the parent job. It only resets the GxMaster's status,
     * making it eligible for the GX scheduler to pick it up again.
     *
     * @param gxMasterId The ID of the GxMaster to retry.
     */
    private void retryGxMaster(final Long gxMasterId) {
        log.info("Attempting to retry GxMaster ID: {}", gxMasterId);
        GxMaster gxMaster = gxMasterRepository.findById(gxMasterId).orElseThrow(
                () -> new RetryFailedException("GxMaster with ID " + gxMasterId + " not found."));

        if (gxMaster.getGxStatus() != GxStatus.ERROR) {
            throw new RetryFailedException("Cannot retry: GxMaster is not in an UPLOAD_FAILED state. Current state: " +
                                           gxMaster.getGxStatus());
        }

        // We still check the parent job to prevent retrying tasks from a job that has been permanently finalized.
        ProcessingJob job = gxMaster.getSourceFile().getProcessingJob();
        validateParentJobIsRetryable(job);

        // Reset the GxMaster's status and error. The scheduler will pick it up.
        // NO CHANGES are made to the parent ProcessingJob.
        gxMaster.setGxStatus(GxStatus.QUEUED_FOR_UPLOAD);
        gxMaster.setErrorMessage(null);
        gxMasterRepository.save(gxMaster);
        log.info("Successfully reset GxMaster ID {} to QUEUED_FOR_UPLOAD. The scheduler will retry it.", gxMasterId);
    }

    /**
     * Ensures that a task is not retried if its parent job is already in a final state.
     */
    private void validateParentJobIsRetryable(ProcessingJob job) {
        if (job.getStatus() == ProcessingStatus.COMPLETED || job.getStatus() == ProcessingStatus.TERMINATED) {
            throw new RetryFailedException(
                    "Cannot retry: The parent job (ID " + job.getId() + ") is already in a final state (" +
                    job.getStatus() + ").");
        }
    }

    /**
     * Sends a message to the SQS queue to re-process a FileMaster.
     */
    private void requeueFileMaster(FileMaster fileMaster) {
        final String groupId = String.valueOf(fileMaster.getGxBucketId());
        // Generate a new deduplication ID to ensure the message is not discarded.
        final String dedupeId = "retry-file-master-" + fileMaster.getId() + "-" + UUID.randomUUID();

        sqsTemplate.send(fileQueueName, MessageBuilder.withPayload(Map.of("fileMasterId", fileMaster.getId()))
                                                      .setHeader(SQS_MESSAGE_GROUP_ID_HEADER, groupId)
                                                      .setHeader(SQS_MESSAGE_DEDUPLICATION_ID_HEADER, dedupeId)
                                                      .build());
    }
}
