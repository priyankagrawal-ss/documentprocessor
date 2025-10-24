package com.eyelevel.documentprocessor.service.sqs;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Manages administrative operations for SQS queues, such as purging.
 */
@Slf4j
@Service
public class SqsManagerService {

    private final SqsAsyncClient sqsAsyncClient;
    private final String zipQueueName;
    private final String fileQueueName;

    public SqsManagerService(SqsAsyncClient sqsAsyncClient, @Value("${aws.sqs.zip-queue-name}") String zipQueueName,
                             @Value("${aws.sqs.file-queue-name}") String fileQueueName) {
        this.sqsAsyncClient = sqsAsyncClient;
        this.zipQueueName = zipQueueName;
        this.fileQueueName = fileQueueName;
    }

    /**
     * Purges all messages from all configured processing queues.
     * WARNING: This is a destructive, irreversible action. It deletes ALL messages
     * in the queues, not just those related to a specific job. It should only be
     * used for system-wide emergency stops. There can be up to a 60-second delay
     * before the queue is fully cleared.
     */
    public void purgeAllQueues() {
        log.warn("ADMIN ACTION: Purging all messages from all processing queues.");
        List<String> queueNames = List.of(zipQueueName, fileQueueName);

        for (String queueName : queueNames) {
            try {
                log.info("Requesting purge for queue: {}", queueName);
                CompletableFuture<Void> purgeFuture = sqsAsyncClient.getQueueUrl(
                                                                            GetQueueUrlRequest.builder().queueName(queueName).build()).thenCompose(response -> {
                                                                        String queueUrl = response.queueUrl();
                                                                        log.warn("Executing purge on queue URL: {}", queueUrl);
                                                                        return sqsAsyncClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(queueUrl).build());
                                                                    }).thenAccept(response -> log.info("Purge request accepted for queue: {}", queueName))
                                                                    .exceptionally(ex -> {
                                                                        log.error("Failed to purge queue: {}",
                                                                                  queueName, ex);
                                                                        return null;
                                                                    });
                purgeFuture.join(); // Wait for the async operation to complete
            } catch (Exception e) {
                log.error("An error occurred while initiating purge for queue: {}", queueName, e);
            }
        }
    }
}
