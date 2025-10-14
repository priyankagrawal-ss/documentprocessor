package com.eyelevel.documentprocessor.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.io.InputStream;
import java.net.URL;
import java.time.Duration;

/**
 * A service for interacting with AWS S3 for file storage, retrieval, and URL pre-signing.
 */
@Slf4j
@Service
public class S3StorageService {

    private final S3Client s3Client;
    private final S3Presigner s3Presigner;
    private final String bucketName;
    private final long presignedUrlDurationMinutes;

    public S3StorageService(
            final S3Client s3Client,
            final S3Presigner s3Presigner,
            @Value("${aws.s3.bucket}") final String bucketName,
            @Value("${aws.s3.presigned-url-duration-minutes}") final long presignedUrlDurationMinutes) {
        this.s3Client = s3Client;
        this.s3Presigner = s3Presigner;
        this.bucketName = bucketName;
        this.presignedUrlDurationMinutes = presignedUrlDurationMinutes;
        log.info("S3StorageService initialized for bucket '{}' with a pre-signed URL duration of {} minutes.",
                bucketName, presignedUrlDurationMinutes);
    }

    /**
     * Generates a pre-signed URL that grants temporary permission to upload a file to a specific S3 key.
     *
     * @param s3Key The full S3 key where the object will be stored.
     * @return A {@link URL} for the pre-signed upload request.
     */
    public URL generatePresignedUploadUrl(final String s3Key) {
        log.debug("Generating pre-signed upload URL for S3 key: {}", s3Key);
        final PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        final PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                .signatureDuration(Duration.ofMinutes(presignedUrlDurationMinutes))
                .putObjectRequest(objectRequest)
                .build();

        return s3Presigner.presignPutObject(presignRequest).url();
    }

    /**
     * Generates a pre-signed URL that grants temporary permission to download a file from a specific S3 key.
     *
     * @param s3Key The full S3 key of the object to download.
     * @return A {@link URL} for the pre-signed download request.
     */
    public URL generatePresignedDownloadUrl(final String s3Key) {
        log.debug("Generating pre-signed download URL for S3 key: {}", s3Key);
        final GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        final GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(Duration.ofMinutes(presignedUrlDurationMinutes))
                .getObjectRequest(getObjectRequest)
                .build();

        return s3Presigner.presignGetObject(presignRequest).url();
    }

    /**
     * Downloads an object from S3 as an {@link InputStream}. The caller is responsible for closing the stream.
     *
     * @param s3Key The S3 key of the object to download.
     * @return An {@link InputStream} of the object's content.
     */
    public InputStream downloadStream(final String s3Key) {
        log.debug("Downloading object from S3 key: {}", s3Key);
        final GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();
        return s3Client.getObject(getObjectRequest);
    }

    /**
     * Uploads content from an {@link InputStream} to a specified S3 key.
     *
     * @param s3Key         The destination S3 key.
     * @param inputStream   The stream of content to upload.
     * @param contentLength The exact length of the content in the stream.
     */
    public void upload(final String s3Key, final InputStream inputStream, final long contentLength) {
        log.debug("Uploading {} bytes to S3 key: {}", contentLength, s3Key);
        final PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();
        s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(inputStream, contentLength));
        log.info("Successfully uploaded object to S3 key: {}", s3Key);
    }

    /**
     * Copies an object within S3 to a structured "gxFiles" destination path.
     *
     * @param sourceKey    The key of the object to copy.
     * @param destFileName The desired filename for the destination object.
     * @param gxBucketId   The client's context bucket ID.
     * @param jobId        The current processing job ID.
     * @return The S3 key of the newly copied object.
     */
    public String copyToGxFiles(final String sourceKey, final String destFileName, final Integer gxBucketId, final long jobId) {
        final String destKey = constructS3Key(destFileName, gxBucketId, jobId, "gxFiles");
        log.info("Copying S3 object from '{}' to '{}'", sourceKey, destKey);
        final CopyObjectRequest copyReq = CopyObjectRequest.builder()
                .sourceBucket(bucketName)
                .sourceKey(sourceKey)
                .destinationBucket(bucketName)
                .destinationKey(destKey)
                .build();
        s3Client.copyObject(copyReq);
        log.info("Successfully copied processed file from {} to {}", sourceKey, destKey);
        return destKey;
    }

    /**
     * Constructs a structured and unique S3 key for organizing files.
     * This method sanitizes the filename and handles different paths for bulk vs. single jobs.
     *
     * @param fileName   The original filename.
     * @param gxBucketId (Optional) The context bucket ID. If null, a "bulk" path is used.
     * @param jobId      The current processing job ID.
     * @param type       The type of path (e.g., "source", "files", "zip", "gxFiles").
     * @return A formatted string representing the full S3 key.
     */
    public static String constructS3Key(final String fileName, final Integer gxBucketId, final Long jobId, final String type) {
        final String safeFileName = fileName.replaceAll("[^a-zA-Z0-9.\\-_]", "_");
        if (gxBucketId == null) {
            return switch (type) {
                case "zip", "source" -> String.format("bulk/%s/%d/%s", type, jobId, safeFileName);
                case "files", "gxFiles" -> String.format("bulk/files/%d/%s", jobId, safeFileName);
                default -> throw new IllegalArgumentException("Invalid S3 path type specified: " + type);
            };
        } else {
            return switch (type) {
                case "zip", "source" -> String.format("%d/%s/%d/%s", gxBucketId, type, jobId, safeFileName);
                case "files", "gxFiles" -> String.format("%d/files/%d/%s", gxBucketId, jobId, safeFileName);
                default -> throw new IllegalArgumentException("Invalid S3 path type specified: " + type);
            };
        }
    }
}