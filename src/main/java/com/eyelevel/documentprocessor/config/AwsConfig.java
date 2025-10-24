package com.eyelevel.documentprocessor.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.crt.S3CrtRetryConfiguration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

/**
 * Configures and provides AWS SDK client beans for S3 and SQS.
 * This configuration dynamically selects the credential strategy based on the active Spring profile.
 */
@Slf4j
@Configuration
public class AwsConfig {

    @Value("${aws.region}")
    private String awsRegion;

    @Value("${aws.access-key:}")
    private String accessKey;

    @Value("${aws.secret-key:}")
    private String secretKey;

    @Value("${aws.s3.retry-count}") // default to 4 if not set
    private int s3RetryCount;

    /**
     * Determines which credentials provider to use based on the active Spring profile.
     */
    @Bean
    public AwsCredentialsProvider awsCredentialsProvider(Environment environment) {
        if (environment.acceptsProfiles(Profiles.of("local"))) {
            log.info("Local profile active. Using StaticCredentialsProvider.");
            if (!StringUtils.hasText(accessKey) || !StringUtils.hasText(secretKey)) {
                throw new IllegalArgumentException(
                        "aws.access-key and aws.secret-key must be set for the 'local' profile.");
            }
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        } else {
            log.info("Non-local profile active. Using DefaultCredentialsProvider (for IAM role).");
            return DefaultCredentialsProvider.create();
        }
    }

    /**
     * Defines a shared ClientOverrideConfiguration with an Adaptive Retry Policy.
     * This is used for all standard AWS clients (like the synchronous S3Client).
     */
    @Bean
    public ClientOverrideConfiguration clientOverrideConfiguration() {
        RetryPolicy adaptiveRetryPolicy = RetryPolicy.forRetryMode(RetryMode.ADAPTIVE).toBuilder()
                                                     // Increase total attempts to 5 (1 initial + 4 retries) for more resilience.
                                                     .numRetries(s3RetryCount).build();

        return ClientOverrideConfiguration.builder().retryPolicy(adaptiveRetryPolicy).build();
    }

    /**
     * Creates the standard synchronous S3Client with a robust retry policy.
     */
    @Bean
    public S3Client s3Client(AwsCredentialsProvider credentialsProvider,
                             ClientOverrideConfiguration clientOverrideConfig) {
        log.info("Configuring AWS S3Client for region: {}", awsRegion);
        return S3Client.builder().credentialsProvider(credentialsProvider) // FIXED: Use dynamic credentials
                       .region(Region.of(awsRegion)).overrideConfiguration(clientOverrideConfig).build();
    }

    /**
     * Creates the high-performance asynchronous S3 client (CRT-based) with a custom retry configuration.
     * NOTE: The CRT client uses its own specific retry mechanism, not the standard one.
     */
    @Bean
    public S3AsyncClient s3AsyncClient(AwsCredentialsProvider credentialsProvider) {
        log.info("Configuring AWS S3AsyncClient (CRT) for region: {}", awsRegion);
        // The CRT client is special and has its own retry configuration method.
        S3CrtRetryConfiguration crtRetryConfiguration = S3CrtRetryConfiguration.builder().numRetries(
                                                                                       s3RetryCount) // Set the number of retries for the CRT client.
                                                                               .build();

        return S3AsyncClient.crtBuilder().credentialsProvider(credentialsProvider) // FIXED: Use dynamic credentials
                            .region(Region.of(awsRegion)) // FIXED: Use dynamic region
                            .retryConfiguration(crtRetryConfiguration).build();
    }

    /**
     * Creates the S3TransferManager, which inherits the configuration from the S3AsyncClient.
     */
    @Bean
    public S3TransferManager s3TransferManager(S3AsyncClient s3AsyncClient) {
        return S3TransferManager.builder().s3Client(s3AsyncClient).build();
    }

    /**
     * Creates the S3 presigner client.
     */
    @Bean
    public S3Presigner s3Presigner(AwsCredentialsProvider credentialsProvider) {
        log.info("Configuring AWS S3Presigner for region: {}", awsRegion);
        return S3Presigner.builder().region(Region.of(awsRegion)).credentialsProvider(credentialsProvider).build();
    }

    /**
     * Creates the SQS async client.
     */
    @Bean
    public SqsAsyncClient sqsAsyncClient(AwsCredentialsProvider credentialsProvider) {
        log.info("Configuring AWS SqsAsyncClient for region: {}", awsRegion);
        return SqsAsyncClient.builder().region(Region.of(awsRegion)).credentialsProvider(credentialsProvider).build();
    }
}