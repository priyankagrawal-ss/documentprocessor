package com.eyelevel.documentprocessor.consumer;

import com.eyelevel.documentprocessor.exception.MessageProcessingFailedException;
import com.eyelevel.documentprocessor.service.DocumentPipelineService;
import com.eyelevel.documentprocessor.service.FileLockingService;
import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * An SQS message consumer that listens for and processes individual file conversion jobs.
 * It is responsible for locking the file record and invoking the main document processing pipeline.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FileConversionConsumer {

    private final FileLockingService fileLockingService;
    private final DocumentPipelineService documentPipelineService;

    /**
     * Listens to the designated SQS queue for file processing messages.
     * <p>
     * This method orchestrates the processing of a single file by first attempting to
     * acquire a transactional lock on the file's database record. If successful, it delegates
     * the heavy lifting to the {@link DocumentPipelineService}.
     *
     * @param message The SQS message payload, expected to contain a "fileMasterId".
     */
    @SqsListener(value = "${aws.sqs.file-queue-name}")
    public void processFileMessage(@Payload final Map<String, Object> message) {
        log.debug("Received new message on file processing queue: {}", message);

        final Object idObject = message.get("fileMasterId");
        if (!(idObject instanceof Number)) {
            log.error("[FATAL] SQS message is invalid or missing 'fileMasterId'. Message will be dropped. Payload: {}", message);
            return;
        }

        final Long fileMasterId = ((Number) idObject).longValue();
        log.info("Received file processing task for FileMaster ID: {}", fileMasterId);

        if (!fileLockingService.acquireLock(fileMasterId)) {
            log.warn("Could not acquire lock for FileMaster ID: {}. The file may already be processed or is not in a queued state.", fileMasterId);
            return;
        }

        log.info("Successfully acquired lock. Starting document pipeline for FileMaster ID: {}", fileMasterId);
        try {
            documentPipelineService.runPipeline(fileMasterId);
            log.info("Successfully completed pipeline for FileMaster ID: {}", fileMasterId);
        } catch (final Exception e) {
            // The JobLifecycleManager is called within the pipeline service to handle the failure state.
            // This exception is re-thrown to leverage the SQS retry/DLQ mechanism for transient errors.
            log.error("Pipeline execution failed for FileMaster ID: {}. Re-throwing to trigger SQS retry.", fileMasterId, e);
            throw new MessageProcessingFailedException("Pipeline processing failed for FileMaster ID " + fileMasterId, e);
        }
    }
}