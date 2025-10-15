package com.eyelevel.documentprocessor.service;

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
import org.springframework.util.CollectionUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;

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

    private static final UUID NIL_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");

    @Transactional
    public void runPipeline(final Long fileMasterId) {
        log.info("Starting document pipeline for FileMaster ID: {}", fileMasterId);
        final FileMaster fileMaster = fileMasterRepository.findById(fileMasterId)
                .orElseThrow(() -> new IllegalStateException("FileMaster not found with ID: " + fileMasterId));

        Path tempFile = null;
        try {
            tempFile = Files.createTempFile("pipeline-" + fileMasterId + "-", "-" + fileMaster.getFileName());

            if (!prepareAndValidateFile(fileMaster, tempFile)) {
                return; // Stop if validation failed.
            }

            if (handleDuplicates(fileMaster)) {
                return; // Stop if the file is a duplicate.
            }

            List<ExtractedFileItem> newItems = findAndExecuteHandler(fileMaster, tempFile);

            // If newItems is null, it means the file type was unsupported and already handled. Stop.
            if (CollectionUtils.isEmpty(newItems)) {
                return;
            }

            processHandlerResults(newItems, fileMaster);

            // Only mark as COMPLETED if it wasn't already set to a different terminal state.
            if (fileMaster.getFileProcessingStatus() == FileProcessingStatus.IN_PROGRESS || fileMaster.getFileProcessingStatus() == FileProcessingStatus.QUEUED) {
                fileMaster.setFileProcessingStatus(FileProcessingStatus.COMPLETED);
                log.info("Pipeline completed successfully for FileMaster ID: {}.", fileMasterId);
            }

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

    private boolean prepareAndValidateFile(FileMaster fileMaster, Path tempFile) throws IOException {
        final long bytesDownloaded = Files.copy(s3StorageService.downloadStream(fileMaster.getFileLocation()), tempFile, StandardCopyOption.REPLACE_EXISTING);
        fileMaster.setFileSize(bytesDownloaded);

        final String validationError = validationService.validateFile(fileMaster.getFileName(), bytesDownloaded);
        if (validationError != null) {
            log.warn("[IGNORED] FileMaster ID {} is invalid. Reason: {}", fileMaster.getId(), validationError);
            updateFileStatusToIgnored(fileMaster, validationError, bytesDownloaded);
            return false;
        }

        if (fileMaster.getOriginalContentHash() == null) {
            final String initialHash = DigestUtils.sha256Hex(Files.newInputStream(tempFile));
            log.info("Calculated initial SHA-256 hash for FileMaster ID {}: {}", fileMaster.getId(), initialHash);
            fileMaster.setOriginalContentHash(initialHash);
            fileMaster.setFileHash(initialHash);
        }
        return true;
    }

    /**
     * Checks for duplicates by comparing the new file's hash against both original and final
     * hashes of previously completed files.
     *
     * @return {@code true} if a duplicate was found and handled, {@code false} otherwise.
     */
    private boolean handleDuplicates(FileMaster fileMaster) {
        final String currentHash = fileMaster.getOriginalContentHash();

        final List<FileMaster> duplicates = fileMasterRepository.findCompletedDuplicateByHash(
                fileMaster.getGxBucketId(),
                currentHash,
                FileProcessingStatus.COMPLETED);

        Optional<FileMaster> firstDuplicate = duplicates.stream()
                .filter(fm -> !fm.getId().equals(fileMaster.getId()))
                .findFirst();

        if (firstDuplicate.isPresent()) {
            FileMaster original = firstDuplicate.get();
            log.warn("Duplicate content detected for FileMaster ID {}. The original is FileMaster ID {}. Marking as SKIPPED_DUPLICATE.",
                    fileMaster.getId(), original.getId());
            fileMaster.setFileHash(original.getFileHash());
            updateFileStatusToSkipped(fileMaster, original.getId());
            return true;
        }
        return false;
    }

    private List<ExtractedFileItem> findAndExecuteHandler(FileMaster fileMaster, Path tempFile) throws IOException {
        final Optional<FileHandler> handlerOpt = fileHandlerFactory.getHandler(fileMaster.getExtension());

        if (handlerOpt.isEmpty()) {
            String errorMessage = "File type '%s' is not supported.".formatted(fileMaster.getExtension());
            log.warn("[IGNORED] FileMaster ID {} will be ignored. Reason: {}", fileMaster.getId(), errorMessage);
            updateFileStatusToIgnored(fileMaster, errorMessage, fileMaster.getFileSize());
            return Collections.emptyList();
        }

        final FileHandler handler = handlerOpt.get();
        log.info("Invoking handler '{}' for FileMaster ID {}.", handler.getClass().getSimpleName(), fileMaster.getId());
        return handler.handle(Files.newInputStream(tempFile), fileMaster);
    }

    private void processHandlerResults(List<ExtractedFileItem> newItems, FileMaster fileMaster) {
        if (newItems.isEmpty()) {
            createOrUpdateGxMasterRecord(fileMaster, null);
        } else if (newItems.size() == 1 && newItems.getFirst().getFilename().equals(fileMaster.getFileName())) {
            final ExtractedFileItem transformedItem = newItems.getFirst();
            final String newHash = DigestUtils.sha256Hex(transformedItem.getContent());
            log.info("File content was transformed. Updating hash for FileMaster ID {} to {}.", fileMaster.getId(), newHash);
            fileMaster.setFileHash(newHash);
            createOrUpdateGxMasterRecord(fileMaster, transformedItem);
        } else if (newItems.size() > 1 && fileMaster.getExtension().equalsIgnoreCase("pdf")) {
            log.info("PDF handler split FileMaster ID {} into {} parts. Creating a GxMaster record for each part.", fileMaster.getId(), newItems.size());
            newItems.forEach(item -> createGxMasterForSplitPart(fileMaster, item));
        } else {
            for (final ExtractedFileItem item : newItems) {
                createFileMasterFromExtractedItem(item, fileMaster.getProcessingJob());
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
            processedFileName = transformedContent.getFilename();
            finalS3Key = S3StorageService.constructS3Key(
                    processedFileName, sourceFile.getGxBucketId(),
                    sourceFile.getProcessingJob().getId(), "files");
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
            final GxMaster gxRecord = existingGxMaster.get();
            gxRecord.setGxStatus(targetStatus);
            gxRecord.setFileSize(finalSize);
            gxRecord.setFileLocation(finalS3Key);
            gxRecord.setProcessedFileName(processedFileName);
            gxRecord.setGxProcessId(processId);
            gxMasterRepository.save(gxRecord);
        } else {
            final String gxFilesS3Key = s3StorageService.copyToGxFiles(finalS3Key, processedFileName,
                    sourceFile.getGxBucketId(), sourceFile.getProcessingJob().getId());

            final GxMaster newGxRecord = GxMaster.builder()
                    .sourceFile(sourceFile)
                    .gxBucketId(sourceFile.getGxBucketId())
                    .fileLocation(gxFilesS3Key)
                    .processedFileName(processedFileName)
                    .fileSize(finalSize)
                    .extension(FilenameUtils.getExtension(processedFileName).toLowerCase())
                    .gxStatus(targetStatus)
                    .gxProcessId(processId)
                    .build();
            gxMasterRepository.save(newGxRecord);
        }
    }

    private void createGxMasterForSplitPart(final FileMaster sourceFile, final ExtractedFileItem splitPart) {
        final byte[] content = splitPart.getContent();
        final long finalSize = content.length;
        final String processedFileName = splitPart.getFilename();

        final String s3Key = S3StorageService.constructS3Key(
                processedFileName, sourceFile.getGxBucketId(),
                sourceFile.getProcessingJob().getId(), "files");
        s3StorageService.upload(s3Key, new ByteArrayInputStream(content), finalSize);

        final boolean isSkipped = sourceFile.getProcessingJob().isSkipGxProcess();
        final GxStatus targetStatus = isSkipped ? GxStatus.SKIPPED : GxStatus.QUEUED_FOR_UPLOAD;
        final UUID processId = isSkipped ? NIL_UUID : null;

        final String gxFilesS3Key = s3StorageService.copyToGxFiles(s3Key, processedFileName,
                sourceFile.getGxBucketId(), sourceFile.getProcessingJob().getId());

        final GxMaster newGxRecord = GxMaster.builder()
                .sourceFile(sourceFile)
                .gxBucketId(sourceFile.getGxBucketId())
                .fileLocation(gxFilesS3Key)
                .processedFileName(processedFileName)
                .fileSize(finalSize)
                .extension(FilenameUtils.getExtension(processedFileName).toLowerCase())
                .gxStatus(targetStatus)
                .gxProcessId(processId)
                .build();
        gxMasterRepository.save(newGxRecord);
    }

    private void createFileMasterFromExtractedItem(final ExtractedFileItem item, final ProcessingJob parentJob) {
        final byte[] content = item.getContent();
        final String initialHash = DigestUtils.sha256Hex(content);
        final Integer gxBucketId = parentJob.getGxBucketId();

        final List<FileMaster> duplicates = fileMasterRepository.findCompletedDuplicateByHash(
                gxBucketId,
                initialHash,
                FileProcessingStatus.COMPLETED);

        if (!duplicates.isEmpty()) {
            log.warn("Duplicate content detected for extracted item '{}'. It matches existing FileMaster ID {}. Skipping creation.",
                    item.getFilename(), duplicates.getFirst().getId());
            return;
        }

        final String newS3Key = S3StorageService.constructS3Key(item.getFilename(), gxBucketId, parentJob.getId(), "files");
        s3StorageService.upload(newS3Key, new ByteArrayInputStream(content), content.length);

        final FileMaster newFileMaster = FileMaster.builder()
                .processingJob(parentJob)
                .gxBucketId(gxBucketId)
                .fileName(item.getFilename())
                .fileSize((long) content.length)
                .extension(FilenameUtils.getExtension(item.getFilename()).toLowerCase())
                .fileHash(initialHash)
                .originalContentHash(initialHash)
                .fileLocation(newS3Key)
                .fileProcessingStatus(FileProcessingStatus.QUEUED)
                .build();
        fileMasterRepository.save(newFileMaster);
        log.info("Created new FileMaster ID: {} for extracted item '{}'.", newFileMaster.getId(), item.getFilename());

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                sqsTemplate.send(fileQueueName, Map.of("fileMasterId", newFileMaster.getId()));
            }
        });
    }
}