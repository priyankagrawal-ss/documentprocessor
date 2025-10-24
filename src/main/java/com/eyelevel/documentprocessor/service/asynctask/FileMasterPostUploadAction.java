package com.eyelevel.documentprocessor.service.asynctask;

import com.eyelevel.documentprocessor.model.FileMaster;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.service.job.JobLifecycleManager;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Implements the post-upload logic for a FileMaster entity.
 * On success, it queues the file for further processing.
 * On failure, it marks the FileMaster record as FAILED.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FileMasterPostUploadAction implements PostUploadAction {

    private static final String SQS_MESSAGE_GROUP_ID_HEADER = "message-group-id";
    private static final String SQS_MESSAGE_DEDUPLICATION_ID_HEADER = "message-deduplication-id";

    private final FileMasterRepository fileMasterRepository;
    private final JobLifecycleManager jobLifecycleManager;
    private final SqsTemplate sqsTemplate;

    @Value("${aws.sqs.file-queue-name}")
    private String fileQueueName;

    @Override
    public void onUploadSuccess(Long fileMasterId) {
        log.info("Async S3 upload successful for FileMaster ID: {}. Queueing for processing.", fileMasterId);
        fileMasterRepository.findById(fileMasterId).ifPresent(this::queueFileForProcessing);
    }

    @Override
    public void onUploadFailure(Long fileMasterId, Throwable error) {
        log.error("Async S3 upload failed for FileMaster ID: {}. Marking record as FAILED.", fileMasterId, error);
        jobLifecycleManager.failJobForFileProcessing(fileMasterId, "Failed during asynchronous S3 upload: " + error.getMessage());
    }

    private void queueFileForProcessing(FileMaster fileMaster) {
        final String groupId = String.valueOf(fileMaster.getGxBucketId());
        final String dedupeId = groupId + "-" + fileMaster.getFileHash();
        sqsTemplate.send(fileQueueName, MessageBuilder.withPayload(Map.of("fileMasterId", fileMaster.getId()))
                .setHeader(SQS_MESSAGE_GROUP_ID_HEADER, groupId)
                .setHeader(SQS_MESSAGE_DEDUPLICATION_ID_HEADER, dedupeId)
                .build());
        log.info("Successfully queued FileMaster ID {} for processing.", fileMaster.getId());
    }
}
