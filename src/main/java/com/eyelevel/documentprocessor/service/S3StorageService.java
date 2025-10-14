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
 * A service for interacting with AWS S3 for file storage and retrieval.
 */
@Service
@Slf4j
public class S3StorageService {

    private final S3Client s3Client;
    private final S3Presigner s3Presigner;
    private final String bucketName;
    private final long presignedUrlDuration;

    public S3StorageService(S3Client s3Client, S3Presigner s3Presigner,
                            @Value("${aws.s3.bucket}") String bucketName,
                            @Value("${aws.s3.presigned-url-duration-minutes}") long presignedUrlDuration) {
        this.s3Client = s3Client;
        this.s3Presigner = s3Presigner;
        this.bucketName = bucketName;
        this.presignedUrlDuration = presignedUrlDuration;
        log.info("S3StorageService initialized for bucket: {}", bucketName);
    }

    /**
     * Generates a pre-signed URL that allows a client to upload a file directly to S3.
     *
     * @param s3Key The full S3 key where the object will be stored.
     * @return A {@link URL} object representing the pre-signed upload URL.
     */
    public URL generatePresignedUploadUrl(String s3Key) {
        log.debug("Generating pre-signed URL for S3 key: {}", s3Key);
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                .signatureDuration(Duration.ofMinutes(presignedUrlDuration))
                .putObjectRequest(objectRequest)
                .build();

        return s3Presigner.presignPutObject(presignRequest).url();
    }

    /**
     * Generates a pre-signed URL that allows a client to download a file directly from S3.
     *
     * @param s3Key The full S3 key of the object to download.
     * @return A {@link URL} object representing the pre-signed download URL.
     */
    public URL generatePresignedDownloadUrl(String s3Key) {
        log.debug("Generating pre-signed download URL for S3 key: {}", s3Key);

        // Build the GetObjectRequest
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        // Build the presign request with duration
        GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(Duration.ofMinutes(presignedUrlDuration))
                .getObjectRequest(getObjectRequest)
                .build();

        // Generate and return the presigned URL
        URL presignedUrl = s3Presigner.presignGetObject(presignRequest).url();
        log.info("Generated pre-signed download URL for S3 key: {}", s3Key);
        return presignedUrl;
    }

    /**
     * Downloads an object from S3 as an InputStream.
     *
     * @param s3Key The S3 key of the object to download.
     * @return An {@link InputStream} of the object's content.
     */
    public InputStream downloadStream(String s3Key) {
        log.debug("Downloading object from S3 key: {}", s3Key);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();
        return s3Client.getObject(getObjectRequest);
    }

    /**
     * Uploads content from an InputStream to a specified S3 key.
     *
     * @param s3Key         The destination S3 key.
     * @param inputStream   The stream of content to upload.
     * @param contentLength The length of the content in the stream.
     */
    public void upload(String s3Key, InputStream inputStream, long contentLength) {
        log.debug("Uploading {} bytes to S3 key: {}", contentLength, s3Key);
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();
        s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(inputStream, contentLength));
        log.info("Successfully uploaded object to S3 key: {}", s3Key);
    }

    /**
     * Copies an object within S3 to a "gxFiles" destination path.
     *
     * @param sourceKey    The key of the object to copy.
     * @param destFileName The desired filename for the destination object.
     * @param gxBucketId   The client's context bucket ID.
     * @param jobId        The current processing job ID.
     * @return The S3 key of the newly copied object.
     */
    String copyToGxFiles(String sourceKey, String destFileName, Integer gxBucketId, long jobId) {
        String destKey = constructS3Key(destFileName, gxBucketId, jobId, "gxFiles");
        log.info("Copying S3 object from '{}' to '{}'", sourceKey, destKey);
        CopyObjectRequest copyReq = CopyObjectRequest.builder()
                .sourceBucket(bucketName)
                .sourceKey(sourceKey)
                .destinationBucket(bucketName)
                .destinationKey(destKey)
                .build();
        s3Client.copyObject(copyReq);
        log.info("Successfully copied terminal file from {} to {}", sourceKey, destKey);
        return destKey;
    }

    /**
     * Constructs a structured and unique S3 key.
     * This helps organize files in S3 and prevents key collisions. It now handles bulk uploads where gxBucketId is null.
     *
     * @param fileName   The original filename.
     * @param gxBucketId The client's context bucket ID (can be null for bulk uploads).
     * @param jobId      The current processing job ID.
     * @param type       The type of path to construct (e.g., "source", "files", "zip").
     * @return A formatted string representing the full S3 key.
     */
    public static String constructS3Key(String fileName, Integer gxBucketId, Long jobId, String type) {
        String safeFileName = fileName.replaceAll("[^a-zA-Z0-9.\\-_]", "_");
        if (gxBucketId == null) {
            // Use a "bulk" prefix for bulk jobs that don't have a bucket ID at the job level.
            return switch (type) {
                case "zip", "source" -> String.format("bulk/%s/%d/%s", type, jobId, safeFileName);
                case "files", "gxFiles" ->
                        String.format("bulk/%s/%d/%s/%s", type, jobId, "unknown_bucket", safeFileName); // Fallback path
                default -> throw new IllegalArgumentException("Invalid S3 path type specified: " + type);
            };
        } else {
            return switch (type) {
                case "zip" -> String.format("%d/zip/%d/%s", gxBucketId, jobId, safeFileName);
                case "files" -> String.format("%d/files/%d/%s", gxBucketId, jobId, safeFileName);
                case "gxFiles" -> String.format("%d/gxFiles/%d/%s", gxBucketId, jobId, safeFileName);
                case "source" -> String.format("%d/source/%d/%s", gxBucketId, jobId, safeFileName);
                default -> throw new IllegalArgumentException("Invalid S3 path type specified: " + type);
            };
        }
    }
}