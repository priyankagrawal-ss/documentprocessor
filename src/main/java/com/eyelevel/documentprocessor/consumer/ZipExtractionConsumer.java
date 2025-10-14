package com.eyelevel.documentprocessor.consumer;

import com.eyelevel.documentprocessor.exception.DocumentProcessingException;
import com.eyelevel.documentprocessor.exception.MessageProcessingFailedException;
import com.eyelevel.documentprocessor.service.JobFailureManager;
import com.eyelevel.documentprocessor.service.ZipProcessingWorker;
import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * SQS message consumer that orchestrates the processing of ZIP archives.
 * It delegates the transactional work to a dedicated worker service.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ZipExtractionConsumer {

    private final ZipProcessingWorker zipProcessingWorker;
    private final JobFailureManager jobFailureManager;

    /**
     * Listens to the ZIP processing SQS queue. This method is non-transactional and acts only as an orchestrator.
     *
     * @param message The SQS message payload, expected to contain a zipMasterId.
     */
    @SqsListener(value = "${aws.sqs.zip-queue-name}")
    public void processZipMessage(@Payload Map<String, Object> message) {
        Object idObject = message.get("zipMasterId");
        if (idObject == null) {
            log.error("[FATAL] Received SQS message for ZIP processing with no 'zipMasterId'. Message will be dropped. Payload: {}", message);
            return;
        }
        Long zipMasterId = ((Number) idObject).longValue();
        log.info("Received ZIP extraction task for ZipMaster ID: {}", zipMasterId);

        try {
            // Delegate the entire transactional operation to the worker.
            zipProcessingWorker.lockAndProcessZip(zipMasterId);
        } catch (DocumentProcessingException e) {
            // This is a terminal failure (like invalid bulk structure). Mark as failed but do not retry.
            log.error("Terminal error processing ZipMaster ID: {}. Marking job as FAILED.", zipMasterId, e);
            jobFailureManager.markZipJobAsFailed(zipMasterId, e.getMessage());
        } catch (Exception e) {
            // This is a potentially transient failure (like corrupt zip, network issue).
            // Mark as failed and trigger SQS retry.
            log.error("Transient error processing ZipMaster ID: {}. Marking job as FAILED and triggering retry.", zipMasterId, e);
            jobFailureManager.markZipJobAsFailed(zipMasterId, e.getMessage());
            throw new MessageProcessingFailedException("Failed to process ZIP for ZipMaster ID " + zipMasterId, e);
        }
    }
}