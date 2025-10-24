package com.eyelevel.documentprocessor.config;

import io.awspring.cloud.sqs.config.SqsMessageListenerContainerFactory;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.time.Duration;

@Configuration
public class SqsListenerConfig {
    /**
     * Creates a high-throughput container factory specifically for the file conversion listener.
     * It reads its settings from the 'app.sqs.listener.file-conversion' properties.
     */
    @Bean("zipProcessContainerFactory") // The bean name is critical!
    public SqsMessageListenerContainerFactory<Object> fileConversionContainerFactory(SqsAsyncClient sqsAsyncClient,
                                                                                     @Value("${app.sqs.listener.zip-process-queue.concurrency-limit}")
                                                                                     int concurrency,
                                                                                     @Value("${app.sqs.listener.zip-process-queue.max-messages-per-poll}")
                                                                                     int maxMessagesPerPoll,
                                                                                     @Value("${app.sqs.listener.zip-process-queue.poll-timeout-seconds}")
                                                                                     int pollTimeoutSeconds) {

        SqsMessageListenerContainerFactory<Object> factory = new SqsMessageListenerContainerFactory<>();
        factory.setSqsAsyncClient(sqsAsyncClient);

        // Apply the custom settings
        factory.configure(options -> options.acknowledgementMode(AcknowledgementMode.ON_SUCCESS)
                                            .maxConcurrentMessages(concurrency).maxMessagesPerPoll(maxMessagesPerPoll)
                                            .pollTimeout(Duration.ofSeconds(pollTimeoutSeconds)));
        return factory;
    }

    /**
     * Creates a low-priority container factory for a hypothetical notification listener.
     * It reads its settings from the 'app.sqs.listener.notifications' properties.
     */
    @Bean("fileProcessContainerFactory") // A different bean with a different name
    public SqsMessageListenerContainerFactory<Object> notificationContainerFactory(SqsAsyncClient sqsAsyncClient,
                                                                                   @Value("${app.sqs.listener.file-process-queue.concurrency-limit}")
                                                                                   int concurrency,
                                                                                   @Value("${app.sqs.listener.file-process-queue.max-messages-per-poll}")
                                                                                   int maxMessagesPerPoll,
                                                                                   @Value("${app.sqs.listener.file-process-queue.poll-timeout-seconds}")
                                                                                   int pollTimeoutSeconds) {

        SqsMessageListenerContainerFactory<Object> factory = new SqsMessageListenerContainerFactory<>();
        factory.setSqsAsyncClient(sqsAsyncClient);

        // Apply a different set of custom settings
        factory.configure(options -> options.acknowledgementMode(AcknowledgementMode.ON_SUCCESS)
                                            .maxConcurrentMessages(concurrency).maxMessagesPerPoll(maxMessagesPerPoll)
                                            .pollTimeout(Duration.ofSeconds(pollTimeoutSeconds)));
        return factory;
    }
}
