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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;

/**
 * Core service that orchestrates the multi-step processing pipeline for a single file.
 * This includes validation, bucket-scoped duplicate checking, content handling,
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
                return;
            }

            if (handleDuplicates(fileMaster)) {
                return;
            }

            List<ExtractedFileItem> newItems = findAndExecuteHandler(fileMaster, tempFile);

            if (newItems == null) {
                return;
            }

            if (newItems.isEmpty()) {
                createOrUpdateGxMasterRecord(fileMaster, null);
            } else {
                processHandlerResults(newItems, fileMaster);
            }

            if (Arrays.asList(FileProcessingStatus.IN_PROGRESS, FileProcessingStatus.QUEUED).contains(fileMaster.getFileProcessingStatus())) {
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
     * Handles duplicate files within the same bucket, with nuanced logic for reprocessing failures.
     *
     * @param fileMaster The current file being processed.
     * @return {@code true} if a duplicate was found and handled (i.e., the file should be skipped),
     * {@code false} if processing should continue.
     */
    private boolean handleDuplicates(FileMaster fileMaster) {
        final String currentHash = fileMaster.getOriginalContentHash();
        final Integer bucketId = fileMaster.getGxBucketId();

        // 1. Use a single query to find all files with the same hash in the same bucket.
        List<FileMaster> allFilesWithSameHash = fileMasterRepository.findAllByGxBucketIdAndOriginalContentHash(bucketId, currentHash);

        // 2. Check for a successfully COMPLETED historical duplicate. This is the highest priority check.
        Optional<FileMaster> completedDuplicate = allFilesWithSameHash.stream()
                .filter(fm -> !fm.getId().equals(fileMaster.getId())) // Exclude the current file
                .filter(fm -> fm.getFileProcessingStatus() == FileProcessingStatus.COMPLETED)
                .findFirst();

        if (completedDuplicate.isPresent()) {
            FileMaster original = completedDuplicate.get();
            log.warn("Bucket-scoped historical duplicate detected for FileMaster ID {}. A COMPLETED version (ID {}) already exists. Marking as SKIPPED.",
                    fileMaster.getId(), original.getId());
            fileMaster.setFileHash(original.getFileHash());
            updateFileStatusToSkipped(fileMaster, original.getId());
            return true; // <<< SKIP
        }

        // 3. Handle concurrent duplicates (race condition).
        // Find all *active* files (including the current one) to see if there's a race.
        List<FileMaster> activeDuplicates = allFilesWithSameHash.stream()
                .filter(fm -> fm.getFileProcessingStatus() == FileProcessingStatus.QUEUED ||
                              fm.getFileProcessingStatus() == FileProcessingStatus.IN_PROGRESS)
                .toList();

        if (activeDuplicates.size() > 1) {
            // A race condition is happening. Deterministically pick the one with the lowest ID to proceed.
            long winningId = activeDuplicates.stream()
                    .mapToLong(FileMaster::getId)
                    .min()
                    .orElse(fileMaster.getId()); // Should never be empty here

            if (fileMaster.getId() != winningId) {
                log.warn("Bucket-scoped concurrent duplicate detected for FileMaster ID {}. The winning processor is ID {}. Marking as SKIPPED.",
                        fileMaster.getId(), winningId);
                updateFileStatusToSkipped(fileMaster, winningId);
                return true;
            }
        }

        // 4. If we reach this point, it means:
        //    a) No COMPLETED version exists.
        //    b) Either there's no race condition, or this is the "winning" file in a race.
        // This means any other duplicates must be in a FAILED, IGNORED, or other non-active state.
        // Therefore, we should allow this file to be processed.
        log.info("Duplicate hash found for FileMaster ID {}, but no COMPLETED or active concurrent version exists. Allowing processing.", fileMaster.getId());
        return false;
    }

    private List<ExtractedFileItem> findAndExecuteHandler(FileMaster fileMaster, Path tempFile) throws IOException, OfficeException {
        final Optional<FileHandler> handlerOpt = fileHandlerFactory.getHandler(fileMaster.getExtension());

        if (handlerOpt.isEmpty()) {
            String errorMessage = "File type '%s' is not supported.".formatted(fileMaster.getExtension());
            log.warn("[IGNORED] FileMaster ID {} will be ignored. Reason: {}", fileMaster.getId(), errorMessage);
            updateFileStatusToIgnored(fileMaster, errorMessage, fileMaster.getFileSize());
            return null;
        }

        final FileHandler handler = handlerOpt.get();
        log.info("Invoking handler '{}' for FileMaster ID {}.", handler.getClass().getSimpleName(), fileMaster.getId());
        return handler.handle(Files.newInputStream(tempFile), fileMaster);
    }

    /**
     * Processes the results from a FileHandler, diversifying logic based on the number
     * and type of items returned. This version correctly handles transformations by creating
     * a new FileMaster record.
     */
    private void processHandlerResults(List<ExtractedFileItem> newItems, FileMaster fileMaster) {
        // Case 1: Handler did nothing or was a terminal transformer that didn't change content.
        if (newItems.isEmpty()) {
            createOrUpdateGxMasterRecord(fileMaster, null);
        }
        // Case 2: Handler optimized the file in-place.
        else if (newItems.size() == 1 && newItems.get(0).getFilename().equals(fileMaster.getFileName())) {
            final ExtractedFileItem transformedItem = newItems.get(0);
            final String newHash = DigestUtils.sha256Hex(transformedItem.getContent());
            if (!newHash.equals(fileMaster.getFileHash())) {
                log.info("File content was optimized. Updating hash for FileMaster ID {}.", fileMaster.getId());
                fileMaster.setFileHash(newHash);
            }
            createOrUpdateGxMasterRecord(fileMaster, transformedItem);
        }
        // Case 3 (OPTIMIZATION): Handler split the file into multiple parts.
        // Bypasses the FileMaster queue and creates GxMaster records directly.
        else if (newItems.size() > 1 && fileMaster.getExtension().equalsIgnoreCase("pdf")) {
            log.info("PDF handler split FileMaster ID {} into {} parts. Creating GxMaster records directly.", newItems.size(), fileMaster.getId());
            for (final ExtractedFileItem item : newItems) {
                createGxMasterForSplitPart(fileMaster, item);
            }
        }
        // Case 4: Handler transformed or extracted one or more new files.
        else {
            SourceType sourceType = (newItems.size() == 1) ? SourceType.TRANSFORMED : SourceType.EXTRACTED;
            log.info("Handler produced {} new items from FileMaster ID {}. Creating a FileMaster for each.", newItems.size(), fileMaster.getId());
            for (final ExtractedFileItem item : newItems) {
                createFileMasterFromExtractedItem(item, fileMaster.getProcessingJob(), sourceType);
            }
        }
    }

    /**
     * Creates a new {@link GxMaster} record for a split part of a larger source file.
     * This method bypasses the FileMaster queue, uploads the content directly, and creates
     * a GxMaster record linked back to the original source file, enabling a one-to-many relationship.
     *
     * @param sourceFile The original FileMaster that was the source of the split.
     * @param splitPart  The content and filename of the individual part.
     */
    private void createGxMasterForSplitPart(final FileMaster sourceFile, final ExtractedFileItem splitPart) {
        final byte[] content = splitPart.getContent();
        final long finalSize = content.length;
        final String processedFileName = splitPart.getFilename();
        final ProcessingJob parentJob = sourceFile.getProcessingJob();

        // 1. Upload the content of the split part to the main 'files' S3 location.
        final String s3Key = S3StorageService.constructS3Key(
                processedFileName, sourceFile.getGxBucketId(),
                parentJob.getId(), "files");
        s3StorageService.upload(s3Key, new ByteArrayInputStream(content), finalSize);
        log.debug("Uploaded split part '{}' for source FileMaster ID {} to S3 key: {}",
                processedFileName, sourceFile.getId(), s3Key);

        // 2. Determine the final status based on the job's settings.
        final boolean isSkipped = parentJob.isSkipGxProcess();
        final GxStatus targetStatus = isSkipped ? GxStatus.SKIPPED : GxStatus.QUEUED_FOR_UPLOAD;
        final UUID processId = isSkipped ? NIL_UUID : null;

        // 3. Copy the file to the final 'gx-files' location, which is the source for GX ingestion.
        final String gxFilesS3Key = s3StorageService.copyToGxFiles(s3Key, processedFileName,
                sourceFile.getGxBucketId(), parentJob.getId());

        // 4. Build and save the new GxMaster record for this specific part.
        final GxMaster newGxRecord = GxMaster.builder()
                .sourceFile(sourceFile) // IMPORTANT: This links back to the original source file.
                .gxBucketId(sourceFile.getGxBucketId())
                .fileLocation(gxFilesS3Key)
                .processedFileName(processedFileName)
                .fileSize(finalSize)
                .extension(FilenameUtils.getExtension(processedFileName).toLowerCase())
                .gxStatus(targetStatus)
                .gxProcessId(processId)
                .build();
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

    /**
     * Creates a new {@link FileMaster} record for an item that was generated by the system,
     * either through extraction or transformation.
     *
     * @param item       The content and filename of the new item.
     * @param parentJob  The parent ProcessingJob.
     * @param sourceType The origin of this new file (EXTRACTED or TRANSFORMED).
     */
    private void createFileMasterFromExtractedItem(final ExtractedFileItem item, final ProcessingJob parentJob, final SourceType sourceType) {
        final byte[] content = item.getContent();
        final String initialHash = DigestUtils.sha256Hex(content);
        final Integer gxBucketId = parentJob.getGxBucketId();

        final List<FileMaster> duplicates = fileMasterRepository.findCompletedDuplicateInBucketByHash(
                gxBucketId,
                initialHash,
                FileProcessingStatus.COMPLETED);

        if (!duplicates.isEmpty()) {
            log.warn("Bucket-scoped duplicate content detected for system-generated item '{}'. It matches existing FileMaster ID {}. Skipping creation.",
                    item.getFilename(), duplicates.get(0).getId());
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
                .sourceType(sourceType) // Set the correct source type
                .fileProcessingStatus(FileProcessingStatus.QUEUED)
                .build();
        fileMasterRepository.save(newFileMaster);
        log.info("Created new {} FileMaster ID: {} for item '{}'.", sourceType, newFileMaster.getId(), item.getFilename());

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                sqsTemplate.send(fileQueueName, Map.of("fileMasterId", newFileMaster.getId()));
            }
        });
    }
}