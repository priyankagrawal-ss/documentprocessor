package com.eyelevel.documentprocessor.service;

import com.eyelevel.documentprocessor.common.apiclient.gx.GXApiClient;
import com.eyelevel.documentprocessor.dto.gx.creategxbucket.response.GXBucket;
import com.eyelevel.documentprocessor.exception.DocumentProcessingException;
import com.eyelevel.documentprocessor.exception.MessageProcessingFailedException;
import com.eyelevel.documentprocessor.model.*;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.ZipMasterRepository;
import com.eyelevel.documentprocessor.service.zip.ZipStreamProcessor;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.zip.ZipException;

/**
 * Service responsible for the initial ingestion of ZIP archives. It processes a {@link ZipMaster} record,
 * extracts all valid files, creates corresponding {@link FileMaster} records, uploads them to storage,
 * and queues them for individual processing.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ZipIngestionService {

    private final ZipMasterRepository zipMasterRepository;
    private final FileMasterRepository fileMasterRepository;
    private final S3StorageService s3StorageService;
    private final SqsTemplate sqsTemplate;
    private final ValidationService validationService;
    private final GXApiClient gxApiClient;
    private final ZipStreamProcessor zipStreamProcessor;

    @Value("${aws.sqs.file-queue-name}")
    private String fileQueueName;

    private static final int COPY_BUFFER = 8192;

    /**
     * Processes a ZIP archive identified by a {@link ZipMaster} ID. This is the primary transactional
     * method that orchestrates the entire ingestion workflow.
     *
     * @param zipMasterId The ID of the {@link ZipMaster} to process.
     */
    @Transactional
    public void ingestAndQueueFiles(final Long zipMasterId) {
        ZipMaster zipMaster = zipMasterRepository.findById(zipMasterId)
                .orElseThrow(() -> new IllegalStateException("ZipMaster not found with ID: " + zipMasterId));

        if (zipMaster.getZipProcessingStatus() != ZipProcessingStatus.QUEUED_FOR_EXTRACTION) {
            log.warn("ZipMaster ID {} is not in a processable state ({}), skipping.", zipMasterId, zipMaster.getZipProcessingStatus());
            return;
        }

        zipMaster.setZipProcessingStatus(ZipProcessingStatus.EXTRACTION_IN_PROGRESS);
        zipMasterRepository.save(zipMaster);
        log.info("Beginning ingestion for ZipMaster ID: {}", zipMasterId);

        try (InputStream s3InputStream = s3StorageService.downloadStream(zipMaster.getOriginalFilePath())) {
            if (zipMaster.getProcessingJob().isBulkUpload()) {
                log.info("Processing ZipMaster ID {} as a BULK upload.", zipMasterId);
                handleBulkUpload(s3InputStream, zipMaster);
            } else {
                log.info("Processing ZipMaster ID {} as a SINGLE upload.", zipMasterId);
                handleSingleUpload(s3InputStream, zipMaster);
            }
            zipMaster.setZipProcessingStatus(ZipProcessingStatus.EXTRACTION_SUCCESS);
            log.info("Successfully completed ingestion for ZipMaster ID: {}", zipMasterId);
        } catch (ZipException e) {
            log.error("Ingestion failed for ZipMaster ID {}: Invalid ZIP archive format.", zipMasterId, e);
            zipMaster.setZipProcessingStatus(ZipProcessingStatus.EXTRACTION_FAILED);
            zipMaster.setErrorMessage("Invalid ZIP archive: " + e.getMessage());
        } catch (DocumentProcessingException e) {
            log.error("Ingestion failed for ZipMaster ID {}: A processing error occurred.", zipMasterId, e);
            zipMaster.setZipProcessingStatus(ZipProcessingStatus.EXTRACTION_FAILED);
            zipMaster.setErrorMessage(e.getMessage());
        } catch (Exception e) {
            log.error("Ingestion failed for ZipMaster ID {} due to a transient error. A message retry will be triggered.", zipMasterId, e);
            throw new MessageProcessingFailedException("Failed to process ZIP for ZipMaster ID " + zipMasterId, e);
        } finally {
            zipMasterRepository.save(zipMaster);
        }
    }

    /**
     * Handles the extraction logic for a single-bucket upload.
     */
    private void handleSingleUpload(InputStream inputStream, ZipMaster zipMaster) throws IOException {
        BiConsumer<String, Path> fileConsumer = (entryName, tempPath) ->
                createAndQueueFileMaster(entryName, tempPath,
                        zipMaster.getProcessingJob(), zipMaster.getGxBucketId(), zipMaster);
        zipStreamProcessor.process(inputStream, fileConsumer);
    }

    /**
     * Handles the extraction logic for a bulk upload, determining the target bucket from the file path.
     */
    private void handleBulkUpload(InputStream inputStream, ZipMaster zipMaster) throws IOException {
        final Map<String, GXBucket> bucketCache = new HashMap<>();
        BiConsumer<String, Path> fileConsumer = (entryName, tempPath) -> {
            int separator = entryName.indexOf('/');
            if (separator == -1) {
                log.warn("[BULK] Skipping root-level file '{}' in ZipMaster ID {}.", entryName, zipMaster.getId());
                return;
            }
            String bucketName = entryName.substring(0, separator);
            if (bucketName.isBlank() || bucketName.startsWith(".")) {
                log.warn("[BULK] Skipping hidden/blank bucket for file '{}' in ZipMaster ID {}.", entryName, zipMaster.getId());
                return;
            }
            try {
                GXBucket gxBucket = bucketCache.computeIfAbsent(bucketName, b -> {
                    log.info("[BULK] Resolving bucket '{}' for ZipMaster ID {}.", b, zipMaster.getId());
                    return gxApiClient.createGXBucket(b);
                });
                createAndQueueFileMaster(entryName, tempPath,
                        zipMaster.getProcessingJob(), gxBucket.bucket().bucketId(), zipMaster);
            } catch (Exception e) {
                log.error("[BULK] Failed processing entry '{}' for bucket '{}' in ZipMaster ID {}.", entryName, bucketName, zipMaster.getId(), e);
            }
        };
        zipStreamProcessor.process(inputStream, fileConsumer);
    }

    /**
     * Processes a single extracted file: validates it, checks for duplicates, uploads it to S3,
     * creates a FileMaster record, and queues it for the next processing stage.
     */
    private void createAndQueueFileMaster(String normalizedPath, Path tempFile, ProcessingJob job, Integer gxBucketId, ZipMaster zipMaster) {
        String fileName = FilenameUtils.getName(normalizedPath);
        if (fileName.startsWith(".")) {
            return;
        }
        try {
            long fileSize = Files.size(tempFile);
            String extension = FilenameUtils.getExtension(fileName).toLowerCase();

            String validationError = validationService.validateFile(fileName, fileSize);
            if (validationError != null) {
                saveIgnoredFile(job, gxBucketId, zipMaster, fileName, fileSize, extension, validationError);
                return;
            }
            if (!validationService.isFileTypeSupported(extension)) {
                String unsupportedError = "File type '%s' is not supported.".formatted(extension);
                saveIgnoredFile(job, gxBucketId, zipMaster, fileName, fileSize, extension, unsupportedError);
                return;
            }

            String fileHash = computeSha256Hex(tempFile);
            Optional<FileMaster> duplicate = fileMasterRepository.findFirstByGxBucketIdAndFileHashAndFileProcessingStatus(
                    gxBucketId, fileHash, FileProcessingStatus.COMPLETED);

            if (duplicate.isPresent()) {
                saveSkippedDuplicateFile(job, gxBucketId, zipMaster, fileName, fileSize, extension, fileHash, duplicate.get().getId());
                return;
            }

            String s3Key = S3StorageService.constructS3Key(fileName, gxBucketId, job.getId(), "files");
            try (var fis = Files.newInputStream(tempFile)) {
                s3StorageService.upload(s3Key, fis, fileSize);
            }

            FileMaster newFile = FileMaster.builder()
                    .processingJob(job).gxBucketId(gxBucketId)
                    .fileName(fileName).fileSize(fileSize).extension(extension)
                    .fileHash(fileHash).originalContentHash(fileHash)
                    .fileLocation(s3Key).sourceType(SourceType.UPLOADED)
                    .zipMaster(zipMaster).fileProcessingStatus(FileProcessingStatus.QUEUED)
                    .build();
            fileMasterRepository.save(newFile);

            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    log.info("Queueing new FileMaster ID: {} for processing.", newFile.getId());
                    sqsTemplate.send(fileQueueName, Map.of("fileMasterId", newFile.getId()));
                }
            });
        } catch (IOException e) {
            throw new DocumentProcessingException("Failed to process extracted file: " + normalizedPath, e);
        }
    }

    private void saveIgnoredFile(ProcessingJob job, Integer gxBucketId, ZipMaster zipMaster, String fileName, long fileSize, String extension, String errorMessage) {
        log.warn("Ignoring file '{}' from ZipMaster ID {}. Reason: {}", fileName, zipMaster.getId(), errorMessage);
        FileMaster ignored = FileMaster.builder()
                .processingJob(job).gxBucketId(gxBucketId).fileName(fileName).fileSize(fileSize)
                .extension(extension).fileProcessingStatus(FileProcessingStatus.IGNORED)
                .sourceType(SourceType.UPLOADED).zipMaster(zipMaster).errorMessage(errorMessage)
                .fileLocation("N/A - IGNORED").build();
        fileMasterRepository.save(ignored);
    }

    private void saveSkippedDuplicateFile(ProcessingJob job, Integer gxBucketId, ZipMaster zipMaster, String fileName, long fileSize, String extension, String fileHash, Long duplicateOfId) {
        log.info("Skipping duplicate file '{}' from ZipMaster ID {}. Original FileMaster ID: {}", fileName, zipMaster.getId(), duplicateOfId);
        FileMaster skipped = FileMaster.builder()
                .processingJob(job).gxBucketId(gxBucketId).fileName(fileName).fileSize(fileSize)
                .extension(extension).fileHash(fileHash).zipMaster(zipMaster).sourceType(SourceType.UPLOADED)
                .fileProcessingStatus(FileProcessingStatus.SKIPPED_DUPLICATE).duplicateOfFileId(duplicateOfId)
                .fileLocation("N/A - DUPLICATE").build();
        fileMasterRepository.save(skipped);
    }

    private String computeSha256Hex(Path file) throws IOException {
        try (var fis = Files.newInputStream(file)) {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (DigestInputStream dis = new DigestInputStream(fis, md)) {
                byte[] buffer = new byte[COPY_BUFFER];
                while (dis.read(buffer) != -1) {
                }
            }
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder(2 * digest.length);
            for (byte b : digest) sb.append(String.format("%02x", b & 0xff));
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 algorithm not found", e);
        }
    }
}