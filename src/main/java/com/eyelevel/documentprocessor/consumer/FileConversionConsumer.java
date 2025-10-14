package com.eyelevel.documentprocessor.consumer;

import com.eyelevel.documentprocessor.exception.MessageProcessingFailedException;
import com.eyelevel.documentprocessor.service.FileProcessingWorker;
import com.eyelevel.documentprocessor.service.PipelineProcessorService;
import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * SQS message consumer responsible for processing individual file conversion and handling jobs.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class FileConversionConsumer {

    private final FileProcessingWorker fileProcessingWorker;
    private final PipelineProcessorService pipelineProcessorService;

    /**
     * Listens to the file processing SQS queue and processes incoming messages.
     *
     * @param message The SQS message payload, expected to contain a fileMasterId.
     */
    @SqsListener(value = "${aws.sqs.file-queue-name}")
    public void processFileMessage(@Payload Map<String, Object> message) {
        log.debug("Received raw message on file queue: {}", message);

        Object idObject = message.get("fileMasterId");
        if (idObject == null) {
            log.error("[FATAL] Received SQS message for file processing with no 'fileMasterId'. Message will be dropped. Payload: {}", message);
            return;
        }

        Long fileMasterId = ((Number) idObject).longValue();
        log.info("Received file processing task for FileMaster ID: {}", fileMasterId);

        if (!fileProcessingWorker.lockFileForProcessing(fileMasterId)) {
            log.warn("Could not acquire lock for FileMaster ID: {}. The file may already be processed, in progress, or in a non-queued state. Message will be acknowledged.", fileMasterId);
            return;
        }

        log.info("Successfully acquired lock. Starting processing for FileMaster ID: {}", fileMasterId);
        try {
            pipelineProcessorService.processFile(fileMasterId);
            log.info("Successfully completed processing for FileMaster ID: {}", fileMasterId);
        } catch (Exception e) {
            // The JobFailureManager is called inside pipelineProcessorService, so we don't call it here.
            // This exception is primarily to trigger the SQS retry/DLQ mechanism.
            log.error("Pipeline execution failed for FileMaster ID: {}. Exception will be re-thrown to trigger SQS retry.", fileMasterId, e);
            throw new MessageProcessingFailedException("Pipeline processing failed for FileMaster ID " + fileMasterId, e);
        }
    }
}