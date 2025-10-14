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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

/**
 * A dedicated service to handle the transactional processing of a ZIP file.
 * It contains robust error handling for both terminal (non-retryable) and transient (retryable) failures.
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

    /**
     * Extracts files from a ZIP archive and queues them for individual processing.
     */
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

        Path tempFile = null;
        try {
            tempFile = Files.createTempFile("zip-download-" + zipMasterId + "-", ".zip");
            final long bytesDownloaded = Files.copy(s3StorageService.downloadStream(zipMaster.getOriginalFilePath()), tempFile, StandardCopyOption.REPLACE_EXISTING);
            zipMaster.setFileSize(bytesDownloaded);

            try (final ZipFile zipFile = new ZipFile(tempFile.toFile())) {
                if (zipMaster.getProcessingJob().isBulkUpload()) {
                    if (!isBulkZipStructureValid(zipFile, zipMaster.getProcessingJob().getId())) {
                        throw new DocumentProcessingException("Bulk ZIP has an invalid structure (files found at root level or ZIP is empty).");
                    }
                    handleBulkUpload(zipFile, zipMaster);
                } else {
                    handleSingleUpload(zipFile, zipMaster);
                }
            }
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
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (final IOException ioEx) {
                    log.error("Failed to delete temporary zip file: {}", tempFile.toAbsolutePath(), ioEx);
                }
            }
        }
    }

    private boolean isBulkZipStructureValid(final ZipFile zipFile, final Long jobId) {
        final Enumeration<? extends ZipEntry> entries = zipFile.entries();
        if (!entries.hasMoreElements()) {
            log.warn("[JobId: {}] Bulk ZIP is empty. Marking as invalid.", jobId);
            return false;
        }
        while (entries.hasMoreElements()) {
            final ZipEntry entry = entries.nextElement();
            if (!entry.isDirectory() && !entry.getName().contains("/")) {
                log.error("[JobId: {}] Invalid bulk ZIP structure. Found file '{}' at the root level.", jobId, entry.getName());
                return false;
            }
        }
        log.info("[JobId: {}] Bulk ZIP structure validation passed.", jobId);
        return true;
    }

    private void handleSingleUpload(final ZipFile zipFile, final ZipMaster zipMaster) throws IOException {
        final Enumeration<? extends ZipEntry> entries = zipFile.entries();
        while (entries.hasMoreElements()) {
            final ZipEntry entry = entries.nextElement();
            if (entry.isDirectory()) continue;
            try (final InputStream inputStream = zipFile.getInputStream(entry)) {
                final byte[] content = inputStream.readAllBytes();
                createAndQueueFileMaster(new ExtractedFileItem(entry.getName(), content), zipMaster.getProcessingJob(), zipMaster.getGxBucketId(), zipMaster);
            }
        }
    }

    private void handleBulkUpload(final ZipFile zipFile, final ZipMaster zipMaster) throws IOException {
        final Map<String, GXBucket> bucketNameCache = new HashMap<>();
        final Enumeration<? extends ZipEntry> entries = zipFile.entries();
        while (entries.hasMoreElements()) {
            final ZipEntry entry = entries.nextElement();
            if (entry.isDirectory()) continue;

            final String path = entry.getName();
            final int separator = path.indexOf('/');
            if (separator == -1) continue;

            final String bucketName = path.substring(0, separator);
            if (bucketName.isBlank()) continue;

            final GXBucket gxBucket = bucketNameCache.computeIfAbsent(bucketName, gxApiClient::createGXBucket);

            try (final InputStream inputStream = zipFile.getInputStream(entry)) {
                final byte[] content = inputStream.readAllBytes();
                createAndQueueFileMaster(new ExtractedFileItem(path, content), zipMaster.getProcessingJob(), gxBucket.bucket().bucketId(), zipMaster);
            }
        }
    }

    /**
     * Creates, validates, and queues a single extracted file for pipeline processing.
     * It also creates a record for files that are ignored due to validation failures or duplication.
     */
    private void createAndQueueFileMaster(final ExtractedFileItem item, final ProcessingJob parentJob, final Integer gxBucketId, final ZipMaster zipMaster) {
        final byte[] content = item.getContent();
        final long fileSize = content.length;
        final String fileName = FilenameUtils.getName(item.getFilename());

        final String validationError = validationService.validateFile(fileName, fileSize);
        if (validationError != null) {
            log.warn("Extracted file '{}' failed validation and will be ignored. Reason: {}", fileName, validationError);
            final FileMaster ignoredFile = FileMaster.builder()
                    .processingJob(parentJob).gxBucketId(gxBucketId)
                    .fileName(fileName).fileSize(fileSize)
                    .extension(FilenameUtils.getExtension(fileName).toLowerCase())
                    .fileProcessingStatus(FileProcessingStatus.IGNORED)
                    .zipMaster(zipMaster)
                    .errorMessage(validationError).fileLocation("N/A - IGNORED").build();
            fileMasterRepository.save(ignoredFile);
            return;
        }

        final String fileHash = DigestUtils.sha256Hex(content);
        final Optional<FileMaster> completedDuplicate = fileMasterRepository.findFirstByGxBucketIdAndFileHashAndFileProcessingStatus(gxBucketId, fileHash, FileProcessingStatus.COMPLETED);
        if (completedDuplicate.isPresent()) {
            log.warn("Duplicate content detected for extracted item '{}'. Creating a record and marking as SKIPPED.", item.getFilename());
            final FileMaster skippedFile = FileMaster.builder()
                    .processingJob(parentJob).gxBucketId(gxBucketId)
                    .fileName(fileName).fileSize(fileSize)
                    .zipMaster(zipMaster)
                    .extension(FilenameUtils.getExtension(fileName).toLowerCase())
                    .fileHash(fileHash)
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
                .fileHash(fileHash).fileLocation(newS3Key)
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