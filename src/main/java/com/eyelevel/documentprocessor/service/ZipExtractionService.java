package com.eyelevel.documentprocessor.service;

import com.eyelevel.documentprocessor.common.apiclient.gx.GXApiClient;
import com.eyelevel.documentprocessor.dto.gx.creategxbucket.response.GXBucket;
import com.eyelevel.documentprocessor.exception.DocumentProcessingException;
import com.eyelevel.documentprocessor.exception.MessageProcessingFailedException;
import com.eyelevel.documentprocessor.model.*;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.ZipMasterRepository;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

/**
 * A dedicated service to handle the transactional processing of a ZIP file.
 * It processes ZIP archives directly from a stream, handles nested ZIPs recursively,
 * and supports both "single" and "bulk" upload modes while filtering out
 * OS-specific metadata files.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ZipExtractionService {

    private final ZipMasterRepository zipMasterRepository;
    private final FileMasterRepository fileMasterRepository;
    private final S3StorageService s3StorageService;
    private final SqsTemplate sqsTemplate;
    private final ValidationService validationService;
    private final GXApiClient gxApiClient;

    @Value("${aws.sqs.file-queue-name}")
    private String fileQueueName;

    private static final Set<String> IGNORED_FILES = Set.of("__MACOSX", ".DS_Store", "Thumbs.db");

    // ... other methods from extractAndQueueFiles to handleBulkUpload remain the same ...
    @Transactional
    public void extractAndQueueFiles(final Long zipMasterId) {
        final ZipMaster zipMaster = zipMasterRepository.findById(zipMasterId)
                .orElseThrow(() -> new IllegalStateException("ZipMaster not found with ID: " + zipMasterId));

        if (zipMaster.getZipProcessingStatus() != ZipProcessingStatus.QUEUED_FOR_EXTRACTION) {
            log.warn("ZipMaster ID: {} is not in a processable state ({}).", zipMasterId, zipMaster.getZipProcessingStatus());
            return;
        }

        zipMaster.setZipProcessingStatus(ZipProcessingStatus.EXTRACTION_IN_PROGRESS);
        zipMasterRepository.save(zipMaster);
        log.info("Acquired lock and started extraction for ZipMaster ID: {}.", zipMaster.getId());

        try {
            processZipStream(zipMaster);
            zipMaster.setZipProcessingStatus(ZipProcessingStatus.EXTRACTION_SUCCESS);
            log.info("Successfully completed ZIP extraction for ZipMaster ID: {}", zipMasterId);
        } catch (ZipException e) {
            log.error("Terminal error for ZipMaster ID {}: File is corrupted or not a valid ZIP format.", zipMasterId, e);
            zipMaster.setZipProcessingStatus(ZipProcessingStatus.EXTRACTION_FAILED);
            zipMaster.setErrorMessage("File is not a valid ZIP archive or is corrupted: " + e.getMessage());
        } catch (DocumentProcessingException e) {
            log.error("Terminal error for ZipMaster ID {}: A processing rule was violated.", zipMasterId, e);
            zipMaster.setZipProcessingStatus(ZipProcessingStatus.EXTRACTION_FAILED);
            zipMaster.setErrorMessage(e.getMessage());
        } catch (Exception e) {
            log.error("Transient error occurred while processing ZipMaster ID {}. Triggering retry.", zipMasterId, e);
            throw new MessageProcessingFailedException("Failed to process ZIP for ZipMaster ID " + zipMasterId, e);
        } finally {
            zipMasterRepository.save(zipMaster);
        }
    }

    private void processZipStream(final ZipMaster zipMaster) throws IOException {
        try (InputStream s3InputStream = s3StorageService.downloadStream(zipMaster.getOriginalFilePath());
             ZipInputStream zipInputStream = new ZipInputStream(s3InputStream)) {

            if (zipMaster.getProcessingJob().isBulkUpload()) {
                handleBulkUpload(zipInputStream, zipMaster);
            } else {
                handleSingleUpload(zipInputStream, zipMaster);
            }
        }
    }

    private void handleSingleUpload(final ZipInputStream zipInputStream, final ZipMaster zipMaster) throws IOException {
        BiConsumer<ZipEntry, byte[]> fileProcessor = (entry, content) -> {
            ExtractedFileItem item = new ExtractedFileItem(entry.getName(), content);
            createAndQueueFileMaster(item, zipMaster.getProcessingJob(), zipMaster.getGxBucketId(), zipMaster);
        };
        processZipEntries(zipInputStream, fileProcessor, false);
    }

    private void handleBulkUpload(final ZipInputStream zipInputStream, final ZipMaster zipMaster) throws IOException {
        final Map<String, GXBucket> bucketNameCache = new HashMap<>();

        BiConsumer<ZipEntry, byte[]> fileProcessor = (entry, content) -> {
            final String normalizedPath = entry.getName();
            final int separator = normalizedPath.indexOf('/');
            if (separator == -1) {
                log.warn("[BULK] Skipping file '{}' as it appears to be at the root, which is invalid for bulk uploads.", normalizedPath);
                return;
            }

            final String bucketName = normalizedPath.substring(0, separator);
            if (bucketName.isBlank() || bucketName.startsWith(".")) {
                log.warn("[BULK] Skipping file '{}' because its derived root folder '{}' is blank or a hidden directory.", normalizedPath, bucketName);
                return;
            }

            try {
                final GXBucket gxBucket = bucketNameCache.computeIfAbsent(bucketName, b -> {
                    log.info("[BULK] New bucket name '{}' detected. Creating or retrieving its GXBucket.", b);
                    return gxApiClient.createGXBucket(b);
                });
                ExtractedFileItem item = new ExtractedFileItem(normalizedPath, content);
                createAndQueueFileMaster(item, zipMaster.getProcessingJob(), gxBucket.bucket().bucketId(), zipMaster);
            } catch (Exception e) {
                log.error("[BULK] Failed to process bucket '{}' for file '{}'. This file will be skipped. Error: {}",
                        bucketName, normalizedPath, e.getMessage(), e);
            }
        };

        processZipEntries(zipInputStream, fileProcessor, true);
    }


    /**
     * A unified, recursive engine to process entries from any ZipInputStream. This version
     * has been refactored to reduce complexity by delegating logic to helper methods.
     */
    private void processZipEntries(final ZipInputStream zipInputStream, final BiConsumer<ZipEntry, byte[]> fileProcessor, boolean isBulk) throws IOException {
        ZipEntry entry;
        final AtomicBoolean hasProcessedAtLeastOneFile = new AtomicBoolean(false);

        while ((entry = zipInputStream.getNextEntry()) != null) {
            final String normalizedPath = entry.getName().replace('\\', '/');

            if (shouldSkipEntry(entry, normalizedPath)) {
                continue;
            }

            if (isBulk && !hasProcessedAtLeastOneFile.get() && !normalizedPath.contains("/")) {
                throw new DocumentProcessingException("Invalid bulk ZIP structure. Found file '" + normalizedPath + "' at the root level.");
            }

            final byte[] content = zipInputStream.readAllBytes();
            hasProcessedAtLeastOneFile.set(true);

            processSingleEntry(normalizedPath, content, fileProcessor, isBulk);
        }

        if (isBulk && !hasProcessedAtLeastOneFile.get()) {
            throw new DocumentProcessingException("Bulk ZIP is empty or contains only directories and ignored files.");
        }
    }

    /**
     * Determines if a given ZIP entry should be skipped, either because it is a
     * directory or a common OS-specific metadata file.
     *
     * @return {@code true} if the entry should be ignored, {@code false} otherwise.
     */
    private boolean shouldSkipEntry(final ZipEntry entry, final String normalizedPath) {
        if (entry.isDirectory() || normalizedPath.endsWith("/")) {
            return true;
        }

        String[] pathSegments = normalizedPath.split("/");
        String rootName = pathSegments.length > 0 ? pathSegments[0] : normalizedPath;
        String fileName = pathSegments.length > 0 ? pathSegments[pathSegments.length - 1] : normalizedPath;

        if (IGNORED_FILES.contains(rootName) || IGNORED_FILES.contains(fileName)) {
            log.debug("Ignoring OS-specific metadata entry: {}", normalizedPath);
            return true;
        }
        return false;
    }

    /**
     * Processes the content of a single, valid ZIP entry. It either processes the
     * entry recursively if it's a nested ZIP, or passes it to the file processor.
     */
    private void processSingleEntry(final String normalizedPath, final byte[] content, final BiConsumer<ZipEntry, byte[]> fileProcessor, boolean isBulk) throws IOException {
        ZipEntry processedEntry = new ZipEntry(normalizedPath);
        if (normalizedPath.toLowerCase().endsWith(".zip")) {
            log.debug("Found nested ZIP file: {}. Processing recursively.", normalizedPath);
            try (var nestedZipStream = new ZipInputStream(new ByteArrayInputStream(content))) {
                processZipEntries(nestedZipStream, fileProcessor, isBulk);
            }
        } else {
            fileProcessor.accept(processedEntry, content);
        }
    }

    private void createAndQueueFileMaster(final ExtractedFileItem item, final ProcessingJob parentJob, final Integer gxBucketId, final ZipMaster zipMaster) {
        final byte[] content = item.getContent();
        final long fileSize = content.length;
        final String normalizedPath = item.getFilename();
        final String fileName = FilenameUtils.getName(normalizedPath);

        if (fileName.startsWith(".")) {
            log.debug("Ignoring hidden file discovered during final processing: {}", fileName);
            return;
        }

        final String validationError = validationService.validateFile(fileName, fileSize);
        if (validationError != null) {
            log.warn("Extracted file '{}' failed validation and will be ignored. Reason: {}", fileName, validationError);
            final FileMaster ignoredFile = FileMaster.builder()
                    .processingJob(parentJob).gxBucketId(gxBucketId)
                    .fileName(fileName).fileSize(fileSize)
                    .extension(FilenameUtils.getExtension(fileName).toLowerCase())
                    .fileProcessingStatus(FileProcessingStatus.IGNORED)
                    .sourceType(SourceType.UPLOADED)
                    .zipMaster(zipMaster)
                    .errorMessage(validationError).fileLocation("N/A - IGNORED").build();
            fileMasterRepository.save(ignoredFile);
            return;
        }

        final String fileHash = DigestUtils.sha256Hex(content);
        final Optional<FileMaster> completedDuplicate = fileMasterRepository.findFirstByGxBucketIdAndFileHashAndFileProcessingStatus(gxBucketId, fileHash, FileProcessingStatus.COMPLETED);
        if (completedDuplicate.isPresent()) {
            log.warn("Duplicate content detected for extracted item '{}'. Creating a record and marking as SKIPPED.", normalizedPath);
            final FileMaster skippedFile = FileMaster.builder()
                    .processingJob(parentJob).gxBucketId(gxBucketId)
                    .fileName(fileName).fileSize(fileSize)
                    .zipMaster(zipMaster)
                    .extension(FilenameUtils.getExtension(fileName).toLowerCase())
                    .fileHash(fileHash)
                    .sourceType(SourceType.UPLOADED)
                    .fileProcessingStatus(FileProcessingStatus.SKIPPED_DUPLICATE)
                    .duplicateOfFileId(completedDuplicate.get().getId())
                    .fileLocation("N/A - DUPLICATE").build();
            fileMasterRepository.save(skippedFile);
            return;
        }

        final String newS3Key = S3StorageService.constructS3Key(fileName, gxBucketId, parentJob.getId(), "files");
        s3StorageService.upload(newS3Key, new ByteArrayInputStream(content), content.length);

        final FileMaster newFileMaster = FileMaster.builder()
                .processingJob(parentJob).gxBucketId(gxBucketId)
                .fileName(fileName).fileSize(fileSize)
                .extension(FilenameUtils.getExtension(fileName).toLowerCase())
                .fileHash(fileHash)
                .originalContentHash(fileHash)
                .fileLocation(newS3Key)
                .sourceType(SourceType.UPLOADED)
                .zipMaster(zipMaster)
                .fileProcessingStatus(FileProcessingStatus.QUEUED).build();
        fileMasterRepository.save(newFileMaster);

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                sqsTemplate.send(fileQueueName, Map.of("fileMasterId", newFileMaster.getId()));
            }
        });
    }

}