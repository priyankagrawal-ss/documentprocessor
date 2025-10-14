package com.eyelevel.documentprocessor.consumer;

import com.eyelevel.documentprocessor.exception.DocumentProcessingException;
import com.eyelevel.documentprocessor.exception.MessageProcessingFailedException;
import com.eyelevel.documentprocessor.service.ZipExtractionService;
import com.eyelevel.documentprocessor.service.lifecycle.JobLifecycleManager;
import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * An SQS message consumer that orchestrates the processing of ZIP archives.
 * It delegates the transactional extraction work to the {@link ZipExtractionService}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ZipExtractionConsumer {

    private final ZipExtractionService zipExtractionService;
    private final JobLifecycleManager jobLifecycleManager;

    /**
     * Listens to the designated SQS queue for ZIP processing messages.
     * <p>
     * This method distinguishes between two types of failures:
     * <ul>
     *   <li><b>Terminal Failures</b> (e.g., corrupt ZIP, invalid structure): The job is permanently failed.</li>
     *   <li><b>Transient Failures</b> (e.g., temporary S3 issue): An exception is thrown to trigger an SQS retry.</li>
     * </ul>
     *
     * @param message The SQS message payload, expected to contain a "zipMasterId".
     */
    @SqsListener(value = "${aws.sqs.zip-queue-name}")
    public void processZipMessage(@Payload final Map<String, Object> message) {
        final Object idObject = message.get("zipMasterId");
        if (!(idObject instanceof Number)) {
            log.error("[FATAL] SQS message is invalid or missing 'zipMasterId'. Message will be dropped. Payload: {}", message);
            return;
        }

        final Long zipMasterId = ((Number) idObject).longValue();
        log.info("Received ZIP extraction task for ZipMaster ID: {}", zipMasterId);

        try {
            zipExtractionService.extractAndQueueFiles(zipMasterId);
        } catch (DocumentProcessingException e) {
            // This is a terminal failure (e.g., invalid bulk structure). Mark job as FAILED and do not retry.
            log.error("A terminal error occurred processing ZipMaster ID: {}. Marking job as FAILED.", zipMasterId, e);
            jobLifecycleManager.failJobForZipExtraction(zipMasterId, e.getMessage());
        } catch (Exception e) {
            // This is a potentially transient failure. Mark as failed and trigger SQS retry.
            log.error("A transient error occurred processing ZipMaster ID: {}. Marking job as FAILED and triggering retry.", zipMasterId, e);
            jobLifecycleManager.failJobForZipExtraction(zipMasterId, e.getMessage());
            throw new MessageProcessingFailedException("Failed to process ZIP for ZipMaster ID " + zipMasterId, e);
        }
    }
}