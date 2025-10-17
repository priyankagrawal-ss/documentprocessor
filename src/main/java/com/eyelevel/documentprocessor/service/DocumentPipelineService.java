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
import org.jodconverter.core.office.OfficeException;
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
 * This includes validation, bucket-scoped duplicate checking, content handling via {@link FileHandler}s,
 * and preparation for the final upload step to GroundX.
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

    /**
     * Executes the entire processing pipeline for a single file identified by its {@link FileMaster} ID.
     * This method is transactional and serves as the main entry point for file processing workers.
     *
     * @param fileMasterId The ID of the {@link FileMaster} to process.
     */
    @Transactional
    public void runPipeline(final Long fileMasterId) {
        log.info("Beginning document pipeline for FileMaster ID: {}", fileMasterId);
        final FileMaster fileMaster = fileMasterRepository.findById(fileMasterId)
                .orElseThrow(() -> new IllegalStateException("FileMaster not found with ID: " + fileMasterId));

        Path tempFile = null;
        try {
            tempFile = Files.createTempFile("pipeline-" + fileMasterId + "-", "-" + fileMaster.getFileName());
            log.debug("Created temporary file for pipeline processing: {}", tempFile.toAbsolutePath());

            if (!prepareAndValidateFile(fileMaster, tempFile)) {
                return;
            }
            if (handleDuplicates(fileMaster)) {
                return;
            }

            List<ExtractedFileItem> newItems = findAndExecuteHandler(fileMaster, tempFile);
            if (CollectionUtils.isEmpty(newItems)) {
                return;
            }

            log.info("File handler produced {} new item(s) for FileMaster ID: {}", newItems.size(), fileMasterId);
            processHandlerResults(newItems, fileMaster);

            if (Arrays.asList(FileProcessingStatus.IN_PROGRESS, FileProcessingStatus.QUEUED).contains(fileMaster.getFileProcessingStatus())) {
                fileMaster.setFileProcessingStatus(FileProcessingStatus.COMPLETED);
                log.info("Pipeline completed successfully for FileMaster ID: {}.", fileMasterId);
            }
        } catch (final Exception e) {
            log.error("A critical exception aborted the pipeline for FileMaster ID: {}.", fileMasterId, e);
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

    /**
     * Downloads the file from S3, validates its size and name, and computes its initial hash.
     */
    private boolean prepareAndValidateFile(FileMaster fileMaster, Path tempFile) throws IOException {
        log.debug("Downloading S3 object '{}' to temporary file for FileMaster ID: {}", fileMaster.getFileLocation(), fileMaster.getId());
        final long bytesDownloaded = Files.copy(s3StorageService.downloadStream(fileMaster.getFileLocation()), tempFile, StandardCopyOption.REPLACE_EXISTING);
        fileMaster.setFileSize(bytesDownloaded);

        final String validationError = validationService.validateFile(fileMaster.getFileName(), bytesDownloaded);
        if (validationError != null) {
            log.warn("FileMaster ID {} is invalid and will be ignored. Reason: {}", fileMaster.getId(), validationError);
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
     * Handles duplicate file detection within the same bucket, gracefully managing historical duplicates
     * and concurrent race conditions.
     *
     * @param fileMaster The current file being processed.
     * @return {@code true} if a duplicate was found and handled, {@code false} if processing should continue.
     */
    private boolean handleDuplicates(FileMaster fileMaster) {
        final String currentHash = fileMaster.getOriginalContentHash();
        final Integer bucketId = fileMaster.getGxBucketId();

        List<FileMaster> allFilesWithSameHash = fileMasterRepository.findAllByGxBucketIdAndOriginalContentHash(bucketId, currentHash);
        Optional<FileMaster> completedDuplicate = allFilesWithSameHash.stream()
                .filter(fm -> !fm.getId().equals(fileMaster.getId()))
                .filter(fm -> fm.getFileProcessingStatus() == FileProcessingStatus.COMPLETED)
                .findFirst();

        if (completedDuplicate.isPresent()) {
            FileMaster original = completedDuplicate.get();
            log.warn("Historical duplicate detected for FileMaster ID {}. A COMPLETED version (ID {}) already exists. Marking as SKIPPED.",
                    fileMaster.getId(), original.getId());
            fileMaster.setFileHash(original.getFileHash());
            updateFileStatusToSkipped(fileMaster, original.getId());
            return true;
        }

        List<FileMaster> activeDuplicates = allFilesWithSameHash.stream()
                .filter(fm -> fm.getFileProcessingStatus() == FileProcessingStatus.QUEUED ||
                              fm.getFileProcessingStatus() == FileProcessingStatus.IN_PROGRESS)
                .toList();

        if (activeDuplicates.size() > 1) {
            long winningId = activeDuplicates.stream().mapToLong(FileMaster::getId).min().orElse(fileMaster.getId());
            if (fileMaster.getId() != winningId) {
                log.warn("Concurrent duplicate detected for FileMaster ID {}. The winning processor is ID {}. Marking as SKIPPED.",
                        fileMaster.getId(), winningId);
                updateFileStatusToSkipped(fileMaster, winningId);
                return true;
            }
        }

        log.info("No completed or winning concurrent duplicates found for FileMaster ID {}. Proceeding with processing.", fileMaster.getId());
        return false;
    }

    /**
     * Selects and executes the appropriate {@link FileHandler} for the given file.
     */
    private List<ExtractedFileItem> findAndExecuteHandler(FileMaster fileMaster, Path tempFile) throws IOException, OfficeException {
        final Optional<FileHandler> handlerOpt = fileHandlerFactory.getHandler(fileMaster.getExtension());
        if (handlerOpt.isEmpty()) {
            String errorMessage = String.format("File type '%s' is not supported.", fileMaster.getExtension());
            log.warn("No handler found for FileMaster ID {}. It will be ignored. Reason: {}", fileMaster.getId(), errorMessage);
            updateFileStatusToIgnored(fileMaster, errorMessage, fileMaster.getFileSize());
            return Collections.emptyList();
        }

        final FileHandler handler = handlerOpt.get();
        log.info("Invoking handler '{}' for FileMaster ID {}.", handler.getClass().getSimpleName(), fileMaster.getId());
        return handler.handle(Files.newInputStream(tempFile), fileMaster);
    }

    /**
     * Processes the results from a FileHandler, routing the outcome to the correct persistence logic.
     */
    private void processHandlerResults(List<ExtractedFileItem> newItems, FileMaster fileMaster) {
        if (newItems.isEmpty()) {
            createOrUpdateGxMasterRecord(fileMaster, null);
        } else if (newItems.size() == 1 && newItems.getFirst().getFilename().equals(fileMaster.getFileName())) {
            final ExtractedFileItem transformedItem = newItems.getFirst();
            final String newHash = DigestUtils.sha256Hex(transformedItem.getContent());
            if (!newHash.equals(fileMaster.getFileHash())) {
                log.info("File content was optimized. Updating hash for FileMaster ID {}.", fileMaster.getId());
                fileMaster.setFileHash(newHash);
            }
            createOrUpdateGxMasterRecord(fileMaster, transformedItem);
        } else if (newItems.size() > 1 && fileMaster.getExtension().equalsIgnoreCase("pdf")) {
            log.info("PDF handler split FileMaster ID {} into {} parts. Creating GxMaster records directly.", fileMaster.getId(), newItems.size());
            for (final ExtractedFileItem item : newItems) {
                createGxMasterForSplitPart(fileMaster, item);
            }
        } else {
            SourceType sourceType = (newItems.size() == 1) ? SourceType.TRANSFORMED : SourceType.EXTRACTED;
            log.info("Handler produced {} new item(s) from FileMaster ID {}. Creating a FileMaster for each.", newItems.size(), fileMaster.getId());
            for (final ExtractedFileItem item : newItems) {
                createFileMasterFromExtractedItem(item, fileMaster.getProcessingJob(), sourceType);
            }
        }
    }

    /**
     * Creates a new {@link GxMaster} record for a split part of a larger source file, bypassing
     * the standard FileMaster queue for optimization.
     */
    private void createGxMasterForSplitPart(final FileMaster sourceFile, final ExtractedFileItem splitPart) {
        final byte[] content = splitPart.getContent();
        final long finalSize = content.length;
        final String processedFileName = splitPart.getFilename();
        final ProcessingJob parentJob = sourceFile.getProcessingJob();
        final String s3Key = S3StorageService.constructS3Key(processedFileName, sourceFile.getGxBucketId(), parentJob.getId(), "files");
        s3StorageService.upload(s3Key, new ByteArrayInputStream(content), finalSize);

        final boolean isSkipped = parentJob.isSkipGxProcess();
        final GxStatus targetStatus = isSkipped ? GxStatus.SKIPPED : GxStatus.QUEUED_FOR_UPLOAD;
        final UUID processId = isSkipped ? NIL_UUID : null;
        final String gxFilesS3Key = s3StorageService.copyToGxFiles(s3Key, processedFileName, sourceFile.getGxBucketId(), parentJob.getId());

        final GxMaster newGxRecord = GxMaster.builder()
                .sourceFile(sourceFile).gxBucketId(sourceFile.getGxBucketId()).fileLocation(gxFilesS3Key)
                .processedFileName(processedFileName).fileSize(finalSize)
                .extension(FilenameUtils.getExtension(processedFileName).toLowerCase())
                .gxStatus(targetStatus).gxProcessId(processId).build();
        gxMasterRepository.save(newGxRecord);

        log.info("Successfully created GxMaster ID: {} for split part '{}' from source FileMaster ID: {}",
                newGxRecord.getId(), processedFileName, sourceFile.getId());
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
     * Creates or updates a {@link GxMaster} record for a source file, potentially with new content
     * if it was transformed in-place by a handler.
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
            processedFileName = transformedContent.getFilename();
            finalS3Key = S3StorageService.constructS3Key(processedFileName, sourceFile.getGxBucketId(), sourceFile.getProcessingJob().getId(), "files");
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
            log.info("Updating existing GxMaster record for FileMaster ID: {}", sourceFile.getId());
            gxRecord.setGxStatus(targetStatus);
            gxRecord.setFileSize(finalSize);
            gxRecord.setFileLocation(finalS3Key);
            gxRecord.setProcessedFileName(processedFileName);
            gxRecord.setGxProcessId(processId);
            gxMasterRepository.save(gxRecord);
        } else {
            final String gxFilesS3Key = s3StorageService.copyToGxFiles(finalS3Key, processedFileName, sourceFile.getGxBucketId(), sourceFile.getProcessingJob().getId());
            final GxMaster newGxRecord = GxMaster.builder()
                    .sourceFile(sourceFile).gxBucketId(sourceFile.getGxBucketId()).fileLocation(gxFilesS3Key)
                    .processedFileName(processedFileName).fileSize(finalSize)
                    .extension(FilenameUtils.getExtension(processedFileName).toLowerCase())
                    .gxStatus(targetStatus).gxProcessId(processId).build();
            gxMasterRepository.save(newGxRecord);
            log.info("Created new GxMaster record for FileMaster ID: {}", sourceFile.getId());
        }
    }

    /**
     * Creates a new {@link FileMaster} record for an item generated by the system (e.g., from a
     * ZIP extraction or file transformation) and queues it for processing.
     */
    private void createFileMasterFromExtractedItem(final ExtractedFileItem item, final ProcessingJob parentJob, final SourceType sourceType) {
        final byte[] content = item.getContent();
        final String initialHash = DigestUtils.sha256Hex(content);
        final Integer gxBucketId = parentJob.getGxBucketId();
        final List<FileMaster> duplicates = fileMasterRepository.findCompletedDuplicateInBucketByHash(gxBucketId, initialHash, FileProcessingStatus.COMPLETED);

        if (!duplicates.isEmpty()) {
            log.warn("Duplicate content detected for system-generated item '{}'. It matches existing FileMaster ID {}. Skipping creation.",
                    item.getFilename(), duplicates.getFirst().getId());
            return;
        }

        final String newS3Key = S3StorageService.constructS3Key(item.getFilename(), gxBucketId, parentJob.getId(), "files");
        s3StorageService.upload(newS3Key, new ByteArrayInputStream(content), content.length);

        final FileMaster newFileMaster = FileMaster.builder()
                .processingJob(parentJob).gxBucketId(gxBucketId).fileName(item.getFilename())
                .fileSize((long) content.length).extension(FilenameUtils.getExtension(item.getFilename()).toLowerCase())
                .fileHash(initialHash).originalContentHash(initialHash).fileLocation(newS3Key)
                .sourceType(sourceType).fileProcessingStatus(FileProcessingStatus.QUEUED).build();
        fileMasterRepository.save(newFileMaster);
        log.info("Created new {} FileMaster ID: {} for item '{}'.", sourceType, newFileMaster.getId(), item.getFilename());

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                log.info("Queueing new system-generated FileMaster ID: {} for processing.", newFileMaster.getId());
                sqsTemplate.send(fileQueueName, Map.of("fileMasterId", newFileMaster.getId()));
            }
        });
    }
}