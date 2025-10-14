package com.eyelevel.documentprocessor.service;

import com.eyelevel.documentprocessor.exception.DocumentProcessingException;
import com.eyelevel.documentprocessor.exception.MessageProcessingFailedException;
import com.eyelevel.documentprocessor.model.*;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.GxMasterRepository;
import com.eyelevel.documentprocessor.service.handlers.FileHandler;
import com.eyelevel.documentprocessor.service.handlers.factory.FileHandlerFactory;
import com.eyelevel.documentprocessor.service.lifecycle.JobLifecycleManager;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Core service that orchestrates the multi-step processing pipeline for a single file.
 * This includes validation, duplicate checking, content handling (conversion/extraction),
 * and preparation for the final upload step.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DocumentPipelineService {

    private final FileMasterRepository fileMasterRepository;
    private final GxMasterRepository gxMasterRepository;
    private final FileHandlerFactory fileHandlerFactory;
    private final S3StorageService s3StorageService;
    private final SqsTemplate sqsTemplate;
    private final ValidationService validationService;
    private final JobLifecycleManager jobLifecycleManager;

    @Value("${aws.sqs.file-queue-name}")
    private String fileQueueName;

    /**
     * A constant representing the nil UUID (00000000-0000-0000-0000-000000000000).
     */
    private static final UUID NIL_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");

    /**
     * Executes the main processing pipeline for a given file.
     * This method is transactional and handles its own error management by delegating
     * failure state changes to the {@link JobLifecycleManager}.
     *
     * @param fileMasterId The ID of the {@link FileMaster} record to process.
     */
    @Transactional
    public void runPipeline(final Long fileMasterId) {
        log.info("Starting document pipeline for FileMaster ID: {}", fileMasterId);
        final FileMaster fileMaster = fileMasterRepository.findById(fileMasterId)
                .orElseThrow(() -> new IllegalStateException("FileMaster not found with ID: " + fileMasterId));

        Path tempFile = null;
        try {
            tempFile = Files.createTempFile("pipeline-" + fileMasterId + "-", "-" + fileMaster.getFileName());
            final long bytesDownloaded = Files.copy(s3StorageService.downloadStream(fileMaster.getFileLocation()), tempFile, StandardCopyOption.REPLACE_EXISTING);

            final String validationError = validationService.validateFile(fileMaster.getFileName(), bytesDownloaded);
            if (validationError != null) {
                log.warn("[IGNORED] FileMaster ID {} is invalid. Reason: {}", fileMasterId, validationError);
                updateFileStatusToIgnored(fileMaster, validationError, bytesDownloaded);
                return;
            }

            fileMaster.setFileSize(bytesDownloaded);
            fileMaster.setFileHash(DigestUtils.sha256Hex(Files.newInputStream(tempFile)));
            log.info("Calculated SHA-256 hash for FileMaster ID {}: {}", fileMasterId, fileMaster.getFileHash());

            final Optional<FileMaster> duplicate = fileMasterRepository.findFirstByGxBucketIdAndFileHashAndFileProcessingStatus(
                    fileMaster.getGxBucketId(), fileMaster.getFileHash(), FileProcessingStatus.COMPLETED);
            if (duplicate.isPresent()) {
                log.warn("Duplicate content detected for FileMaster ID {}. Marking as SKIPPED_DUPLICATE of FileMaster ID {}.",
                        fileMasterId, duplicate.get().getId());
                updateFileStatusToSkipped(fileMaster, duplicate.get().getId());
                createOrUpdateGxMasterRecord(fileMaster, null); // Call to ensure consistent logic flow
                return;
            }

            final FileHandler handler = fileHandlerFactory.getHandler(fileMaster.getExtension())
                    .orElseThrow(() -> new DocumentProcessingException("File type '%s' is not supported.".formatted(fileMaster.getExtension())));

            log.info("Invoking handler '{}' for FileMaster ID {}.", handler.getClass().getSimpleName(), fileMasterId);
            final List<ExtractedFileItem> newItems = handler.handle(Files.newInputStream(tempFile), fileMaster);

            if (newItems.isEmpty()) {
                createOrUpdateGxMasterRecord(fileMaster, null);
            } else if (newItems.size() == 1 && newItems.getFirst().getFilename().equals(fileMaster.getFileName())) {
                createOrUpdateGxMasterRecord(fileMaster, newItems.getFirst());
            } else {
                for (final ExtractedFileItem item : newItems) {
                    createFileMasterFromExtractedItem(item, fileMaster.getProcessingJob());
                }
            }
            fileMaster.setFileProcessingStatus(FileProcessingStatus.COMPLETED);
            log.info("Pipeline completed successfully for FileMaster ID {}.", fileMasterId);

        } catch (final Exception e) {
            log.error("A critical exception occurred during the processing pipeline for FileMaster ID: {}.", fileMasterId, e);
            jobLifecycleManager.failJobForFileProcessing(fileMasterId, e.getMessage());
            throw new MessageProcessingFailedException("Pipeline processing failed for FileMaster ID " + fileMasterId, e);
        } finally {
            fileMasterRepository.save(fileMaster);
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (final IOException ioException) {
                    log.error("Failed to delete temporary pipeline file: {}", tempFile.toAbsolutePath(), ioException);
                }
            }
        }
    }

    private void updateFileStatusToIgnored(final FileMaster fileMaster, final String reason, final long fileSize) {
        fileMaster.setFileProcessingStatus(FileProcessingStatus.IGNORED);
        fileMaster.setErrorMessage(reason);
        fileMaster.setFileSize(fileSize);
    }

    private void updateFileStatusToSkipped(final FileMaster fileMaster, final Long duplicateOfId) {
        fileMaster.setFileProcessingStatus(FileProcessingStatus.SKIPPED_DUPLICATE);
        fileMaster.setDuplicateOfFileId(duplicateOfId);
    }

    /**
     * Creates or updates the final {@link GxMaster} record for a file that has completed its processing pipeline.
     * This method explicitly skips creation for files marked as duplicates.
     */
    private void createOrUpdateGxMasterRecord(final FileMaster sourceFile, @Nullable final ExtractedFileItem transformedContent) {
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

        final boolean isSkipped = sourceFile.getProcessingJob().isSkipGxProcess();
        final GxStatus targetStatus = isSkipped ? GxStatus.SKIPPED : GxStatus.QUEUED_FOR_UPLOAD;
        final UUID processId = isSkipped ? NIL_UUID : null;

        final Optional<GxMaster> existingGxMaster = gxMasterRepository.findBySourceFileId(sourceFile.getId());

        if (existingGxMaster.isPresent()) {
            log.warn("Updating existing GxMaster record for FileMaster ID {}.", sourceFile.getId());
            final GxMaster gxRecord = existingGxMaster.get();
            gxRecord.setGxStatus(targetStatus);
            gxRecord.setFileSize(finalSize);
            gxRecord.setFileLocation(finalS3Key);
            gxRecord.setGxProcessId(processId);
            gxMasterRepository.save(gxRecord);
        } else {
            log.info("Creating new GxMaster record for source FileMaster ID {}.", sourceFile.getId());
            final String gxFilesS3Key = s3StorageService.copyToGxFiles(finalS3Key, processedFileName,
                    sourceFile.getGxBucketId(), sourceFile.getProcessingJob().getId());

            final GxMaster newGxRecord = GxMaster.builder()
                    .sourceFile(sourceFile)
                    .gxBucketId(sourceFile.getGxBucketId())
                    .fileLocation(gxFilesS3Key)
                    .processedFileName(processedFileName)
                    .fileSize(finalSize)
                    .extension(sourceFile.getExtension())
                    .gxStatus(targetStatus)
                    .gxProcessId(processId)
                    .build();
            gxMasterRepository.save(newGxRecord);
            log.info("Successfully created GxMaster ID: {} for source FileMaster ID: {}", newGxRecord.getId(), sourceFile.getId());
        }
    }

    /**
     * Creates a new {@link FileMaster} record for an item extracted from a container file (e.g., ZIP, MSG).
     */
    private void createFileMasterFromExtractedItem(final ExtractedFileItem item, final ProcessingJob parentJob) {
        log.debug("Creating new FileMaster from extracted item: {}", item.getFilename());
        final byte[] content = item.getContent();
        final String fileHash = DigestUtils.sha256Hex(content);
        final Integer gxBucketId = parentJob.getGxBucketId();
        final Long jobId = parentJob.getId();

        final Optional<FileMaster> completedDuplicate = fileMasterRepository.findFirstByGxBucketIdAndFileHashAndFileProcessingStatus(
                gxBucketId, fileHash, FileProcessingStatus.COMPLETED);
        if (completedDuplicate.isPresent()) {
            log.warn("Duplicate content detected for extracted item '{}'. Skipping creation.", item.getFilename());
            return;
        }

        final String newS3Key = S3StorageService.constructS3Key(item.getFilename(), gxBucketId, jobId, "files");
        s3StorageService.upload(newS3Key, new ByteArrayInputStream(content), content.length);

        final FileMaster newFileMaster = FileMaster.builder()
                .processingJob(parentJob)
                .gxBucketId(gxBucketId)
                .fileName(item.getFilename())
                .fileSize((long) content.length)
                .extension(FilenameUtils.getExtension(item.getFilename()).toLowerCase())
                .fileHash(fileHash)
                .fileLocation(newS3Key)
                .fileProcessingStatus(FileProcessingStatus.QUEUED)
                .build();
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