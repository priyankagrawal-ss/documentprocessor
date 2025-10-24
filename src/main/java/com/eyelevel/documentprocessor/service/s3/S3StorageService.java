package com.eyelevel.documentprocessor.service.s3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.UploadPartPresignRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A service for interacting with AWS S3 for file storage, retrieval, and URL pre-signing.
 * This version uses S3TransferManager for robust uploads to prevent timeouts.
 */
@Slf4j
@Service
public class S3StorageService {

    private final S3Client s3Client;
    private final S3Presigner s3Presigner;
    private final S3TransferManager transferManager;
    private final String bucketName;
    private final long presignedUrlDurationMinutes;

    public S3StorageService(final S3Client s3Client, final S3Presigner s3Presigner,
                            final S3TransferManager transferManager,
                            @Value("${aws.s3.bucket}") final String bucketName,
                            @Value("${aws.s3.presigned-url-duration-minutes}") final long presignedUrlDurationMinutes) {
        this.s3Client = s3Client;
        this.s3Presigner = s3Presigner;
        this.transferManager = transferManager; // ADDED
        this.bucketName = bucketName;
        this.presignedUrlDurationMinutes = presignedUrlDurationMinutes;
        log.info("S3StorageService initialized for bucket '{}' with a pre-signed URL duration of {} minutes.",
                bucketName, presignedUrlDurationMinutes);
    }

    /**
     * Uploads content from an {@link InputStream} to a specified S3 key using S3TransferManager.
     * This method streams the input to a temporary file to work with the async-based Transfer Manager,
     * which prevents both OutOfMemoryErrors and socket timeout exceptions.
     *
     * @param s3Key         The destination S3 key.
     * @param inputStream   The stream of content to upload.
     * @param contentLength The exact length of the content in the stream (used for logging).
     */
    public void upload(final String s3Key, final InputStream inputStream, final long contentLength) {
        Path tempFile = null;
        try {
            // S3TransferManager's async nature works best with files. We stream the input
            // to a temporary file on disk to avoid loading the entire content into memory.
            tempFile = Files.createTempFile("s3-upload-" + UUID.randomUUID(), ".tmp");
            Files.copy(inputStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
            log.debug("Uploading temporary file {} ({} bytes) to S3 key: {}", tempFile.toAbsolutePath(), contentLength,
                    s3Key);

            // Use UploadFileRequest, which is a specialized request for file uploads.
            UploadFileRequest uploadFileRequest = UploadFileRequest.builder().putObjectRequest(
                    req -> req.bucket(bucketName).key(s3Key)).source(tempFile).build();

            // transferManager.uploadFile() is non-blocking.
            FileUpload upload = transferManager.uploadFile(uploadFileRequest);

            // To make this method synchronous for the caller, we block and wait for completion.
            upload.completionFuture().join();

            log.info("Successfully uploaded object to S3 key: {}", s3Key);

        } catch (IOException e) {
            log.error("Failed to create or write to temporary file for S3 upload", e);
            throw new RuntimeException("Failed to process file for S3 upload", e);
        } finally {
            // CRITICAL: Always clean up the temporary file, whether the upload succeeded or failed.
            if (tempFile != null) {
                try {
                    Files.delete(tempFile);
                } catch (IOException e) {
                    log.warn("Failed to delete temporary file: {}", tempFile.toAbsolutePath(), e);
                }
            }
        }
    }

    public CompletableFuture<Void> uploadAsync(final String s3Key, final InputStream inputStream) {
        Path tempFile;
        try {
            // Step 1: Stream the input to a temporary file on disk. This is a fast, local I/O operation.
            tempFile = Files.createTempFile("s3-upload-" + UUID.randomUUID(), ".tmp");
            Files.copy(inputStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            log.error("Failed to create temporary file for S3 upload to key: {}", s3Key, e);
            // If we can't even create the temp file, return a future that is already failed.
            return CompletableFuture.failedFuture(e);
        }

        log.debug("Starting asynchronous upload of temporary file {} to S3 key: {}", tempFile.toAbsolutePath(), s3Key);

        UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                .putObjectRequest(req -> req.bucket(bucketName).key(s3Key))
                .source(tempFile)
                .build();

        // Step 2: Start the upload and get the Future. Do NOT block with .join().
        FileUpload upload = transferManager.uploadFile(uploadFileRequest);
        CompletableFuture<CompletedFileUpload> uploadFuture = upload.completionFuture();

        // Step 3: Chain a cleanup action that will run AFTER the upload is complete (success or failure).
        return uploadFuture.whenComplete((result, throwable) -> {
            try {
                // This block is guaranteed to execute after the upload finishes.
                Files.deleteIfExists(tempFile);
                if (throwable != null) {
                    log.error("Asynchronous S3 upload failed for key: {}", s3Key, throwable);
                } else {
                    log.info("Asynchronous S3 upload completed successfully for key: {}", s3Key);
                }
            } catch (IOException e) {
                log.error("CRITICAL: Failed to delete temporary file after S3 upload: {}", tempFile.toAbsolutePath(), e);
            }
        }).thenApply(v -> null); // Convert CompletableFuture<CompletedFileUpload> to CompletableFuture<Void>
    }


    /**
     * Downloads an object from S3 as an {@link InputStream}. The caller is responsible for closing the stream.
     * This method continues to use the standard S3Client, which is efficient for streaming downloads.
     *
     * @param s3Key The S3 key of the object to download.
     * @return An {@link InputStream} of the object's content.
     */
    public InputStream downloadStream(final String s3Key) {
        log.debug("Downloading object from S3 key: {}", s3Key);
        final GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(s3Key).build();
        return s3Client.getObject(getObjectRequest);
    }


    // ============================================================================================
    // ALL OTHER METHODS REMAIN UNCHANGED
    // ============================================================================================

    public static String constructS3Key(final String fileName, final Integer gxBucketId, final Long jobId,
                                        final String type) {
        final String safeFileName = fileName.replaceAll("[^a-zA-Z0-9.\\-_]", "_");
        if (gxBucketId == null) {
            return switch (type) {
                case "source" -> String.format("bulk/%s/%d/%s", type, jobId, safeFileName);
                case "files", "gxFiles" -> String.format("bulk/files/%d/%s", jobId, safeFileName);
                default -> throw new IllegalArgumentException("Invalid S3 path type specified: " + type);
            };
        } else {
            return switch (type) {
                case "source" -> String.format("%d/%s/%d/%s", gxBucketId, type, jobId, safeFileName);
                case "files", "gxFiles" -> String.format("%d/files/%d/%s", gxBucketId, jobId, safeFileName);
                default -> throw new IllegalArgumentException("Invalid S3 path type specified: " + type);
            };
        }
    }

    public URL generatePresignedUploadUrl(final String s3Key) {
        log.debug("Generating pre-signed upload URL for S3 key: {}", s3Key);
        final PutObjectRequest objectRequest = PutObjectRequest.builder().bucket(bucketName).key(s3Key).build();

        final PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder().signatureDuration(
                Duration.ofMinutes(presignedUrlDurationMinutes)).putObjectRequest(objectRequest).build();

        return s3Presigner.presignPutObject(presignRequest).url();
    }

    public URL generatePresignedDownloadUrl(final String s3Key) {
        log.debug("Generating pre-signed download URL for S3 key: {}", s3Key);
        final GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(s3Key).build();

        final GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder().signatureDuration(
                Duration.ofMinutes(presignedUrlDurationMinutes)).getObjectRequest(getObjectRequest).build();

        return s3Presigner.presignGetObject(presignRequest).url();
    }

    public String copyToGxFiles(final String sourceKey, final String destFileName, final Integer gxBucketId,
                                final long jobId) {
        final String destKey = constructS3Key(destFileName, gxBucketId, jobId, "gxFiles");
        log.info("Copying S3 object from '{}' to '{}'", sourceKey, destKey);
        final CopyObjectRequest copyReq = CopyObjectRequest.builder().sourceBucket(bucketName).sourceKey(sourceKey)
                .destinationBucket(bucketName).destinationKey(destKey)
                .build();
        s3Client.copyObject(copyReq);
        log.info("Successfully copied processed file from {} to {}", sourceKey, destKey);
        return destKey;
    }

    public String initiateMultipartUpload(final String s3Key) {
        log.debug("Initiating multipart upload for S3 key: {}", s3Key);
        final CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();
        final CreateMultipartUploadResponse response = s3Client.createMultipartUpload(createMultipartUploadRequest);
        log.info("Initiated multipart upload with upload ID: {} for S3 key: {}", response.uploadId(), s3Key);
        return response.uploadId();
    }

    public URL generatePresignedUrlForPart(final String s3Key, final String uploadId, final int partNumber) {
        log.debug("Generating pre-signed URL for part #{} of upload ID: {} for S3 key: {}", partNumber, uploadId,
                s3Key);
        final UploadPartRequest uploadPartRequest = UploadPartRequest.builder().bucket(bucketName).key(s3Key)
                .uploadId(uploadId).partNumber(partNumber).build();

        final UploadPartPresignRequest presignRequest = UploadPartPresignRequest.builder().signatureDuration(
                Duration.ofMinutes(presignedUrlDurationMinutes)).uploadPartRequest(uploadPartRequest).build();

        return s3Presigner.presignUploadPart(presignRequest).url();
    }

    public void completeMultipartUpload(final String s3Key, final String uploadId, final List<CompletedPart> parts) {
        log.debug("Completing multipart upload with upload ID: {} for S3 key: {}", uploadId, s3Key);
        final CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder().parts(parts)
                .build();

        final CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .uploadId(
                        uploadId)
                .multipartUpload(
                        completedMultipartUpload)
                .build();

        s3Client.completeMultipartUpload(completeMultipartUploadRequest);
        log.info("Successfully completed multipart upload with upload ID: {} for S3 key: {}", uploadId, s3Key);
    }
}