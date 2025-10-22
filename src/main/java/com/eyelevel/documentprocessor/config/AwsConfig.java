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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/**
 * Configures and provides AWS SDK client beans for S3 and SQS.
 * This configuration dynamically selects the credential strategy based on the active Spring profile.
 * - For the 'local' profile, it uses static access/secret keys.
 * - For all other profiles, it uses the DefaultCredentialsProvider (for IAM roles).
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

    /**
     * The Factory Bean for AWS Credentials.
     * This bean determines which credentials provider to use based on the active Spring profile.
     *
     * @param environment The Spring application environment.
     *
     * @return The configured {@link AwsCredentialsProvider}.
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
     * Creates the S3 client, injecting the dynamically chosen credentials' provider.
     *
     * @param credentialsProvider The strategy bean providing AWS credentials.
     *
     * @return A configured {@link S3Client} instance.
     */
    @Bean
    public S3Client s3Client(AwsCredentialsProvider credentialsProvider) {
        log.info("Configuring AWS S3Client for region: {}", awsRegion);
        return S3Client.builder().region(Region.of(awsRegion)).credentialsProvider(credentialsProvider).build();
    }

    /**
     * Creates the S3 presigner, injecting the dynamically chosen credentials' provider.
     *
     * @param credentialsProvider The strategy bean providing AWS credentials.
     *
     * @return A configured {@link S3Presigner} instance.
     */
    @Bean
    public S3Presigner s3Presigner(AwsCredentialsProvider credentialsProvider) {
        log.info("Configuring AWS S3Presigner for region: {}", awsRegion);
        return S3Presigner.builder().region(Region.of(awsRegion)).credentialsProvider(credentialsProvider).build();
    }

    /**
     * Creates the SQS async client, injecting the dynamically chosen credentials' provider.
     *
     * @param credentialsProvider The strategy bean providing AWS credentials.
     *
     * @return A configured {@link SqsAsyncClient} instance.
     */
    @Bean
    public SqsAsyncClient sqsAsyncClient(AwsCredentialsProvider credentialsProvider) {
        log.info("Configuring AWS SqsAsyncClient for region: {}", awsRegion);
        return SqsAsyncClient.builder().region(Region.of(awsRegion)).credentialsProvider(credentialsProvider).build();
    }
}