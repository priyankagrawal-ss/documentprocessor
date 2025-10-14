package com.eyelevel.documentprocessor.service;

import com.eyelevel.documentprocessor.exception.MessageProcessingFailedException;
import com.eyelevel.documentprocessor.model.*;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.GxMasterRepository;
import com.eyelevel.documentprocessor.service.handlers.FileHandler;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import jakarta.annotation.Nullable;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Core service that orchestrates the multi-step processing pipeline for a single file.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class PipelineProcessorService {

    private final FileMasterRepository fileMasterRepository;
    private final GxMasterRepository gxMasterRepository;
    private final FileHandlerFactory fileHandlerFactory;
    private final S3StorageService s3StorageService;
    private final SqsTemplate sqsTemplate;
    private final ValidationService validationService;
    private final JobFailureManager jobFailureManager;
    private final JobCompletionService jobCompletionService;

    @Value("${aws.sqs.file-queue-name}")
    private String fileQueueName;

    /**
     * Executes the main processing logic for a given file.
     *
     * @param fileMasterId The ID of the FileMaster record to process.
     */
    @Transactional
    public void processFile(Long fileMasterId) {
        log.info("Starting processing pipeline for FileMaster ID: {}", fileMasterId);
        FileMaster fileMaster = fileMasterRepository.findById(fileMasterId)
                .orElseThrow(() -> new IllegalStateException("FileMaster not found with ID: " + fileMasterId));

        Path tempFile = null;
        try {
            tempFile = Files.createTempFile("pipeline-" + fileMasterId + "-", "-" + fileMaster.getFileName());
            long bytesDownloaded;
            try (InputStream inputStream = s3StorageService.downloadStream(fileMaster.getFileLocation())) {
                bytesDownloaded = Files.copy(inputStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
            }

            String validationError = validationService.validateFile(fileMaster.getFileName(), bytesDownloaded);
            if (validationError != null) {
                log.warn("[IGNORED] FileMaster ID {} is invalid. Reason: {}.", fileMasterId, validationError);
                updateFileStatusToIgnored(fileMaster, validationError, bytesDownloaded);
                return;
            }

            fileMaster.setFileSize(bytesDownloaded);
            try (InputStream hashStream = Files.newInputStream(tempFile)) {
                fileMaster.setFileHash(DigestUtils.sha256Hex(hashStream));
            }
            log.info("Calculated SHA-256 hash for FileMaster ID {}: {}", fileMasterId, fileMaster.getFileHash());

            Optional<FileMaster> duplicate = fileMasterRepository.findFirstByGxBucketIdAndFileHashAndFileProcessingStatus(
                    fileMaster.getGxBucketId(), fileMaster.getFileHash(), FileProcessingStatus.COMPLETED);

            if (duplicate.isPresent()) {
                log.warn("Duplicate content detected for FileMaster ID {}. Marking as SKIPPED_DUPLICATE.", fileMasterId);
                updateFileStatusToSkipped(fileMaster, duplicate.get().getId());
                createOrUpdateGxMasterRecord(fileMaster, null);
                return;
            }

            Optional<FileHandler> handlerOpt = fileHandlerFactory.getHandler(fileMaster.getExtension());

            if (handlerOpt.isEmpty()) {
                String unsupportedMessage = "File type '%s' is not supported by any configured handler.".formatted(fileMaster.getExtension());
                log.warn("[IGNORED] FileMaster ID {}: {}", fileMasterId, unsupportedMessage);
                updateFileStatusToIgnored(fileMaster, unsupportedMessage, bytesDownloaded);
                return;
            }

            FileHandler handler = handlerOpt.get();
            log.info("Invoking handler '{}' for FileMaster ID {}.", handler.getClass().getSimpleName(), fileMasterId);
            List<ExtractedFileItem> newItems;
            try (InputStream processStream = Files.newInputStream(tempFile)) {
                newItems = handler.handle(processStream, fileMaster);
            }

            if (newItems.isEmpty()) {
                createOrUpdateGxMasterRecord(fileMaster, null);
            } else if (newItems.size() == 1 && newItems.getFirst().getFilename().equals(fileMaster.getFileName())) {
                createOrUpdateGxMasterRecord(fileMaster, newItems.getFirst());
            } else {
                for (ExtractedFileItem item : newItems) {
                    createFileMasterFromExtractedItem(item, fileMaster.getProcessingJob());
                }
            }

            fileMaster.setFileProcessingStatus(FileProcessingStatus.COMPLETED);
            log.info("Pipeline completed successfully for FileMaster ID {}.", fileMasterId);

        } catch (Exception e) {
            // ###############################################################
            // ### THIS IS THE EXACT POINT WHERE markFileJobAsFailed IS USED ###
            // ###############################################################
            log.error("A critical exception occurred during the processing pipeline for FileMaster ID: {}.", fileMasterId, e);

            // Immediately and durably mark the job as failed in a separate transaction.
            jobFailureManager.markFileJobAsFailed(fileMasterId, e.getMessage());

            // Still throw an exception to trigger the SQS message retry/DLQ mechanism.
            throw new MessageProcessingFailedException("Pipeline processing failed for FileMaster ID " + fileMasterId, e);
            // ###############################################################

        } finally {
            fileMasterRepository.save(fileMaster);
            log.debug("Saved final state for FileMaster ID {}", fileMasterId);

            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException ioException) {
                    log.error("Failed to delete temporary pipeline file: {}", tempFile.toAbsolutePath(), ioException);
                }
            }

            if (fileMaster.getFileProcessingStatus() != FileProcessingStatus.FAILED) {
                final Long jobId = fileMaster.getProcessingJob().getId();
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override
                    public void afterCommit() {
                        jobCompletionService.checkForJobCompletion(jobId);
                    }
                });
            }
        }
    }

    private void updateFileStatusToIgnored(FileMaster fileMaster, String reason, long fileSize) {
        fileMaster.setFileProcessingStatus(FileProcessingStatus.IGNORED);
        fileMaster.setErrorMessage(reason);
        fileMaster.setFileSize(fileSize);
    }

    private void updateFileStatusToSkipped(FileMaster fileMaster, Long duplicateOfId) {
        fileMaster.setFileProcessingStatus(FileProcessingStatus.SKIPPED_DUPLICATE);
        fileMaster.setDuplicateOfFileId(duplicateOfId);
    }

    /**
     * Creates or updates the final GxMaster record for a file that has completed its processing pipeline.
     */
    private void createOrUpdateGxMasterRecord(FileMaster sourceFile, @Nullable ExtractedFileItem transformedContent) {
        if (sourceFile.getFileProcessingStatus() == FileProcessingStatus.SKIPPED_DUPLICATE) {
            log.debug("Skipping GxMaster creation for SKIPPED_DUPLICATE FileMaster ID: {}", sourceFile.getId());
            return;
        }

        String finalS3Key;
        long finalSize;
        String processedFileName = sourceFile.getFileName();

        if (transformedContent != null) {
            finalSize = transformedContent.getContent().length;
            finalS3Key = S3StorageService.constructS3Key(
                    transformedContent.getFilename(), sourceFile.getGxBucketId(),
                    sourceFile.getProcessingJob().getId(), "files");
            log.debug("Uploading transformed content for FileMaster ID {} to S3 key: {}", sourceFile.getId(), finalS3Key);
            s3StorageService.upload(finalS3Key, new ByteArrayInputStream(transformedContent.getContent()), finalSize);
        } else {
            finalS3Key = sourceFile.getFileLocation();
            finalSize = sourceFile.getFileSize();
        }

        Optional<GxMaster> existingGxMaster = gxMasterRepository.findBySourceFileId(sourceFile.getId());
        if (existingGxMaster.isPresent()) {
            log.warn("Updating existing GxMaster record for FileMaster ID {}.", sourceFile.getId());
            GxMaster gxRecord = existingGxMaster.get();
            gxRecord.setGxStatus(sourceFile.getProcessingJob().isSkipGxProcess() ? GxStatus.SKIPPED : GxStatus.QUEUED_FOR_UPLOAD);
            gxRecord.setFileSize(finalSize);
            gxRecord.setFileLocation(finalS3Key);
            gxMasterRepository.save(gxRecord);
        } else {
            log.info("Creating new GxMaster record for source FileMaster ID {}.", sourceFile.getId());
            String gxFilesS3Key = s3StorageService.copyToGxFiles(finalS3Key, processedFileName,
                    sourceFile.getGxBucketId(), sourceFile.getProcessingJob().getId());
            GxMaster newGxRecord = GxMaster.builder()
                    .sourceFile(sourceFile).gxBucketId(sourceFile.getGxBucketId())
                    .fileLocation(gxFilesS3Key).processedFileName(processedFileName)
                    .fileSize(finalSize).extension(sourceFile.getExtension())
                    .gxStatus(sourceFile.getProcessingJob().isSkipGxProcess() ? GxStatus.SKIPPED : GxStatus.QUEUED_FOR_UPLOAD).build();
            gxMasterRepository.save(newGxRecord);
            log.info("Successfully created GxMaster ID: {} for source FileMaster ID: {}", newGxRecord.getId(), sourceFile.getId());
        }
    }

    /**
     * Creates a new FileMaster record for an item extracted from a container file (like a ZIP or MSG).
     */
    private void createFileMasterFromExtractedItem(ExtractedFileItem item, ProcessingJob parentJob) {
        log.debug("Creating new FileMaster from extracted item: {}", item.getFilename());
        byte[] content = item.getContent();
        String fileHash = DigestUtils.sha256Hex(content);
        Integer gxBucketId = parentJob.getGxBucketId();
        Long jobId = parentJob.getId();

        Optional<FileMaster> completedDuplicate = fileMasterRepository.findFirstByGxBucketIdAndFileHashAndFileProcessingStatus(
                gxBucketId, fileHash, FileProcessingStatus.COMPLETED);
        if (completedDuplicate.isPresent()) {
            log.warn("Duplicate content detected for extracted item '{}'. Skipping.", item.getFilename());
            FileMaster skippedFile = FileMaster.builder()
                    .processingJob(parentJob).gxBucketId(gxBucketId)
                    .fileName(item.getFilename()).fileSize((long) content.length)
                    .extension(FilenameUtils.getExtension(item.getFilename()).toLowerCase())
                    .fileHash(fileHash).fileProcessingStatus(FileProcessingStatus.SKIPPED_DUPLICATE)
                    .duplicateOfFileId(completedDuplicate.get().getId()).fileLocation("N/A - DUPLICATE").build();
            fileMasterRepository.save(skippedFile);
            return;
        }

        Optional<FileMaster> failedRecord = fileMasterRepository.findFirstByProcessingJobIdAndFileHashAndFileProcessingStatus(
                jobId, fileHash, FileProcessingStatus.FAILED);
        if (failedRecord.isPresent()) {
            log.warn("Found a previously FAILED record (FileMaster ID {}) with the same hash. Re-queueing it for processing.", failedRecord.get().getId());
            FileMaster existing = failedRecord.get();
            existing.setFileProcessingStatus(FileProcessingStatus.QUEUED);
            existing.setErrorMessage(null);
            fileMasterRepository.save(existing);

            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    sqsTemplate.send(fileQueueName, Map.of("fileMasterId", existing.getId()));
                }
            });
            return;
        }

        String newS3Key = S3StorageService.constructS3Key(item.getFilename(), gxBucketId, jobId, "files");
        s3StorageService.upload(newS3Key, new ByteArrayInputStream(content), content.length);
        FileMaster newFileMaster = FileMaster.builder()
                .processingJob(parentJob).gxBucketId(gxBucketId)
                .fileName(item.getFilename()).fileSize((long) content.length)
                .extension(FilenameUtils.getExtension(item.getFilename()).toLowerCase())
                .fileHash(fileHash).fileLocation(newS3Key)
                .fileProcessingStatus(FileProcessingStatus.QUEUED).build();
        fileMasterRepository.save(newFileMaster);
        log.info("Created new FileMaster ID: {} for extracted item '{}'.", newFileMaster.getId(), item.getFilename());

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                sqsTemplate.send(fileQueueName, Map.of("fileMasterId", newFileMaster.getId()));
                log.info("Successfully queued new FileMaster ID: {} for processing.", newFileMaster.getId());
            }
        });
    }
}