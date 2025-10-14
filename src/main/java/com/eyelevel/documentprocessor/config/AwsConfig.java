package com.eyelevel.documentprocessor.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClientBuilder;

import java.net.URI;
import java.util.Optional;

/**
 * Configures and provides the necessary AWS SDK v2 client beans for S3 and SQS.
 * This configuration uses static credentials provided via application properties.
 */
@Slf4j
@Configuration
public class AwsConfig {

    @Value("${aws.region}")
    private String awsRegion;

    @Value("${aws.access-key}")
    private String accessKey;

    @Value("${aws.secret-key}")
    private String secretKey;

    @Value("${aws.sqs.endpoint-override:#{null}}")
    private Optional<String> sqsEndpointOverride;

    /**
     * Creates the primary synchronous client for interacting with AWS S3.
     *
     * @return A configured {@link S3Client} instance.
     */
    @Bean
    public S3Client s3Client() {
        log.info("Configuring AWS S3Client for region: {}", awsRegion);
        return S3Client.builder()
                .region(Region.of(awsRegion))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .build();
    }

    /**
     * Creates a dedicated client for generating pre-signed URLs for S3 objects.
     *
     * @return A configured {@link S3Presigner} instance.
     */
    @Bean
    public S3Presigner s3Presigner() {
        log.info("Configuring AWS S3Presigner for region: {}", awsRegion);
        return S3Presigner.builder()
                .region(Region.of(awsRegion))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .build();
    }

    /**
     * Creates the primary asynchronous client for interacting with AWS SQS.
     * This bean supports an optional endpoint override, which is useful for connecting
     * to local development environments like LocalStack.
     *
     * @return A configured {@link SqsAsyncClient} instance.
     */
    @Bean
    public SqsAsyncClient sqsAsyncClient() {
        log.info("Configuring AWS SqsAsyncClient for region: {}", awsRegion);
        SqsAsyncClientBuilder clientBuilder = SqsAsyncClient.builder()
                .region(Region.of(awsRegion))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));

        sqsEndpointOverride.ifPresent(endpoint -> {
            log.warn("Applying SQS endpoint override for local testing: {}", endpoint);
            clientBuilder.endpointOverride(URI.create(endpoint));
        });

        return clientBuilder.build();
    }
}