package com.eyelevel.documentprocessor.service;

import com.eyelevel.documentprocessor.common.apiclient.gx.GXApiClient;
import com.eyelevel.documentprocessor.dto.gx.creategxbucket.response.GXBucket;
import com.eyelevel.documentprocessor.exception.DocumentProcessingException;
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
import java.util.zip.ZipFile;

/**
 * A dedicated service to handle the transactional processing of a ZIP file.
 * This class exists to solve transactional self-invocation and race condition issues.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ZipProcessingWorker {

    private final ZipMasterRepository zipMasterRepository;
    private final FileMasterRepository fileMasterRepository;
    private final S3StorageService s3StorageService;
    private final SqsTemplate sqsTemplate;
    private final ValidationService validationService;
    private final JobCompletionService jobCompletionService;
    private final GXApiClient gxApiClient;

    @Value("${aws.sqs.file-queue-name}")
    private String fileQueueName;

    /**
     * Attempts to lock and process a ZIP file within a single, atomic transaction.
     * This method contains the entire success-path logic.
     *
     * @param zipMasterId The ID of the ZipMaster to process.
     * @throws Exception if any part of the processing fails, ensuring the transaction is rolled back.
     */
    @Transactional
    public void lockAndProcessZip(Long zipMasterId) throws Exception {
        ZipMaster zipMaster = zipMasterRepository.findById(zipMasterId)
                .orElseThrow(() -> new IllegalStateException("ZipMaster not found with ID: " + zipMasterId));

        if (zipMaster.getZipProcessingStatus() != ZipProcessingStatus.QUEUED_FOR_EXTRACTION) {
            log.warn("Could not acquire lock for ZipMaster ID: {}. It is not in QUEUED_FOR_EXTRACTION state.", zipMasterId);
            return;
        }

        zipMaster.setZipProcessingStatus(ZipProcessingStatus.EXTRACTION_IN_PROGRESS);
        zipMasterRepository.save(zipMaster);
        log.info("Acquired lock and started processing for ZipMaster ID: {}.", zipMaster.getId());

        Path tempFile = null;
        try {
            tempFile = Files.createTempFile("zip-download-" + zipMasterId + "-", ".zip");
            try (InputStream s3Stream = s3StorageService.downloadStream(zipMaster.getOriginalFilePath())) {
                Files.copy(s3Stream, tempFile, StandardCopyOption.REPLACE_EXISTING);
            }

            try (ZipFile zipFile = new ZipFile(tempFile.toFile())) {
                if (zipMaster.getProcessingJob().isBulkUpload()) {
                    if (!isBulkZipStructureValid(zipFile, zipMaster.getProcessingJob().getId())) {
                        String errorMsg = "Bulk ZIP for Job ID " + zipMaster.getProcessingJob().getId() + " has an invalid structure.";
                        // This is a terminal failure, but we throw an exception to let the caller handle it.
                        throw new DocumentProcessingException(errorMsg);
                    }
                    handleBulkUpload(zipFile, zipMaster);
                } else {
                    handleSingleUpload(zipFile, zipMaster);
                }
            }
            zipMaster.setZipProcessingStatus(ZipProcessingStatus.EXTRACTION_SUCCESS);
            zipMasterRepository.save(zipMaster);

            final Long jobId = zipMaster.getProcessingJob().getId();
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    jobCompletionService.checkForJobCompletion(jobId);
                }
            });
        } finally {
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException ioEx) {
                    log.error("Failed to delete temporary zip file: {}", tempFile.toAbsolutePath(), ioEx);
                }
            }
        }
    }

    private boolean isBulkZipStructureValid(ZipFile zipFile, Long jobId) {
        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        if (!entries.hasMoreElements()) {
            log.warn("[JobId: {}] Bulk ZIP is empty. Marking as invalid.", jobId);
            return false;
        }
        while (entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();
            String name = entry.getName();
            if (!entry.isDirectory() && !name.contains("/")) {
                log.error("[JobId: {}] Invalid bulk ZIP structure. Found file '{}' at the root level.", jobId, name);
                return false;
            }
        }
        log.info("[JobId: {}] Bulk ZIP structure validation passed.", jobId);
        return true;
    }

    private void handleSingleUpload(ZipFile zipFile, ZipMaster zipMaster) throws IOException {
        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        while (entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();
            if (entry.isDirectory()) continue;
            try (InputStream inputStream = zipFile.getInputStream(entry)) {
                byte[] content = inputStream.readAllBytes();
                createAndQueueFileMaster(new ExtractedFileItem(entry.getName(), content), zipMaster.getProcessingJob(), zipMaster.getGxBucketId(), zipMaster);
            }
        }
    }

    private void handleBulkUpload(ZipFile zipFile, ZipMaster zipMaster) throws IOException {
        Map<String, GXBucket> bucketNameCache = new HashMap<>();
        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        while (entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();
            if (entry.isDirectory()) continue;

            String path = entry.getName();
            int separator = path.indexOf('/');
            if (separator == -1) continue;

            String bucketName = path.substring(0, separator);
            if (bucketName.isBlank()) continue;

            GXBucket gxBucket = bucketNameCache.computeIfAbsent(bucketName, gxApiClient::createGXBucket);

            try (InputStream inputStream = zipFile.getInputStream(entry)) {
                byte[] content = inputStream.readAllBytes();
                createAndQueueFileMaster(new ExtractedFileItem(path, content), zipMaster.getProcessingJob(), gxBucket.bucket().bucketId(), zipMaster);
            }
        }
    }

    private void createAndQueueFileMaster(ExtractedFileItem item, ProcessingJob parentJob, Integer gxBucketId, ZipMaster zipMaster) {
        byte[] content = item.getContent();
        long fileSize = content.length;
        String fileName = FilenameUtils.getName(item.getFilename());

        String validationError = validationService.validateFile(fileName, fileSize);
        if (validationError != null) {
            FileMaster ignoredFile = FileMaster.builder()
                    .processingJob(parentJob).gxBucketId(gxBucketId)
                    .fileName(fileName).fileSize(fileSize)
                    .extension(FilenameUtils.getExtension(fileName).toLowerCase())
                    .fileProcessingStatus(FileProcessingStatus.IGNORED)
                    .zipMaster(zipMaster)
                    .errorMessage(validationError).fileLocation("N/A - IGNORED").build();
            fileMasterRepository.save(ignoredFile);
            return;
        }

        String fileHash = DigestUtils.sha256Hex(content);
        Optional<FileMaster> completedDuplicate = fileMasterRepository.findFirstByGxBucketIdAndFileHashAndFileProcessingStatus(gxBucketId, fileHash, FileProcessingStatus.COMPLETED);
        if (completedDuplicate.isPresent()) {
            FileMaster skippedFile = FileMaster.builder()
                    .processingJob(parentJob).gxBucketId(gxBucketId)
                    .fileName(fileName).fileSize(fileSize)
                    .zipMaster(zipMaster)
                    .extension(FilenameUtils.getExtension(fileName).toLowerCase())
                    .fileHash(fileHash).fileProcessingStatus(FileProcessingStatus.SKIPPED_DUPLICATE)
                    .duplicateOfFileId(completedDuplicate.get().getId()).fileLocation("N/A - DUPLICATE").build();
            fileMasterRepository.save(skippedFile);
            return;
        }

        String newS3Key = S3StorageService.constructS3Key(fileName, gxBucketId, parentJob.getId(), "files");
        s3StorageService.upload(newS3Key, new ByteArrayInputStream(content), content.length);

        FileMaster newFileMaster = FileMaster.builder()
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