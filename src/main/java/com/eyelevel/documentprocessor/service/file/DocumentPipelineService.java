package com.eyelevel.documentprocessor.service.file;

import com.eyelevel.documentprocessor.exception.MessageProcessingFailedException;
import com.eyelevel.documentprocessor.model.*;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.GxMasterRepository;
import com.eyelevel.documentprocessor.service.asynctask.AsyncTaskManager;
import com.eyelevel.documentprocessor.service.asynctask.FileMasterPostUploadAction;
import com.eyelevel.documentprocessor.service.asynctask.GxMasterPostUploadAction;
import com.eyelevel.documentprocessor.service.handlers.FileHandler;
import com.eyelevel.documentprocessor.service.handlers.factory.FileHandlerFactory;
import com.eyelevel.documentprocessor.service.job.JobLifecycleManager;
import com.eyelevel.documentprocessor.service.s3.S3StorageService;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.jodconverter.core.office.OfficeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@Service
public class DocumentPipelineService {

    private static final UUID NIL_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");

    // --- Dependencies ---
    private final FileMasterRepository fileMasterRepository;
    private final GxMasterRepository gxMasterRepository;
    private final FileHandlerFactory fileHandlerFactory;
    private final S3StorageService s3StorageService;
    private final JobLifecycleManager jobLifecycleManager;
    private final FileMasterAtomicService fileMasterAtomicService;
    private final ValidationService validationService;
    private final AsyncTaskManager asyncTaskManager;
    private final FileMasterPostUploadAction fileMasterPostUploadAction;
    private final GxMasterPostUploadAction gxMasterPostUploadAction;
    private DocumentPipelineService self;

    private record FileMetadata(String fileName, long fileSize, String extension, String fileHash) {
    }

    public DocumentPipelineService(FileMasterRepository fileMasterRepository, GxMasterRepository gxMasterRepository,
                                   FileHandlerFactory fileHandlerFactory, S3StorageService s3StorageService,
                                   ValidationService validationService, JobLifecycleManager jobLifecycleManager,
                                   FileMasterAtomicService fileMasterAtomicService,
                                   FileMasterPostUploadAction fileMasterPostUploadAction,
                                   GxMasterPostUploadAction gxMasterPostUploadAction,
                                   AsyncTaskManager asyncTaskManager) {
        this.fileMasterRepository = fileMasterRepository;
        this.gxMasterRepository = gxMasterRepository;
        this.fileHandlerFactory = fileHandlerFactory;
        this.s3StorageService = s3StorageService;
        this.validationService = validationService;
        this.jobLifecycleManager = jobLifecycleManager;
        this.fileMasterAtomicService = fileMasterAtomicService;
        this.fileMasterPostUploadAction = fileMasterPostUploadAction;
        this.gxMasterPostUploadAction = gxMasterPostUploadAction;
        this.asyncTaskManager = asyncTaskManager;
    }


    @Autowired
    public void setSelf(@Lazy DocumentPipelineService self) {
        this.self = self;
    }

    public void runPipeline(final Long fileMasterId) {
        log.info("Beginning document pipeline for FileMaster ID: {}", fileMasterId);
        final FileMaster fileMaster = fileMasterRepository.findByIdWithJob(fileMasterId)
                .orElseThrow(() -> new IllegalStateException("Cannot run pipeline: FileMaster not found with ID " + fileMasterId));

        if (isJobTerminated(fileMaster)) {
            return;
        }

        Path tempFile = null;
        try {
            tempFile = Files.createTempFile("pipeline-" + fileMasterId + "-", fileMaster.getFileName());

            // CORE LOGIC FIX: Check if the file has already been hashed.
            if (fileMaster.getFileHash() == null) {
                log.debug("FileMaster ID {} requires hashing. Running duplicate check.", fileMasterId);
                // This is likely a direct upload, so we must perform the check.
                if (!prepareAndCheckForDirectUploadDuplicates(fileMaster, tempFile)) {
                    return; // The file was a duplicate or invalid. Stop processing.
                }
            } else {
                log.debug("FileMaster ID {} already has a hash. Skipping duplicate check.", fileMasterId);
                // This file came from ZipIngestionService. Just download it for the handler.
                try (InputStream s3Stream = s3StorageService.downloadStream(fileMaster.getFileLocation())) {
                    Files.copy(s3Stream, tempFile, StandardCopyOption.REPLACE_EXISTING);
                }
            }

            // Proceed with the rest of the pipeline
            final List<ExtractedFileItem> handlerResults = findAndExecuteHandler(fileMaster, tempFile);
            processHandlerResults(handlerResults, fileMaster);
            self.markAsCompletedIfStillInProgress(fileMasterId);
        } catch (final Exception e) {
            handlePipelineFailure(fileMasterId, e);
        } finally {
            cleanupTempFile(tempFile);
        }
    }

    private void processHandlerResults(final List<ExtractedFileItem> results, final FileMaster sourceFile) {
        final String ext = sourceFile.getExtension() == null ? "" : sourceFile.getExtension().toLowerCase();
        final long sourceId = sourceFile.getId();

        // PDF handler → every result is a final artifact
        if ("pdf".equals(ext)) {
            log.info("PDF handler produced {} result(s) for FileMaster ID {}.", results.size(), sourceId);
            if (results.isEmpty()) {
                self.createOrUpdateGxMasterRecord(sourceFile, null);
            } else {
                results.forEach(item -> self.createGxMasterForFinalArtifact(item, sourceFile));
            }
            return;
        }

        boolean isExtraction = isExtractionRequired(sourceFile, results);
        if (isExtraction) {
            log.info("Handler created {} new file(s) from FileMaster ID {}. Queueing for processing.", results.size(), sourceId);
            final SourceType sourceType = determineSourceType(sourceFile);

            results.forEach(item -> {
                Optional<FileMaster> newFileOpt = self.processNewItemForQueueing(item, sourceFile, sourceType);
                if (newFileOpt.isPresent()) {
                    log.info("New FileMaster created with ID {} for item '{}'.", newFileOpt.get().getId(), item.getFilename());
                } else {
                    log.info("No new FileMaster created for item '{}'. It may be duplicate or ignored.", item.getFilename());
                }
            });
        } else {
            self.createOrUpdateGxMasterRecord(sourceFile, results.isEmpty() ? null : results.getFirst());
        }

    }

    private FileMaster createNewFileMaster(FileMaster parentFile, SourceType sourceType, FileMetadata metadata, String s3Key) {
        ProcessingJob job = parentFile.getProcessingJob();
        Integer gxBucketId = parentFile.getGxBucketId();
        ZipMaster zipMaster = parentFile.getZipMaster();
        FileMaster potentialNewFile = FileMaster.builder()
                .processingJob(job).gxBucketId(gxBucketId).fileName(metadata.fileName())
                .fileSize(metadata.fileSize()).extension(metadata.extension()).fileHash(metadata.fileHash())
                .sourceType(sourceType)
                .zipMaster(zipMaster)
                .fileProcessingStatus(FileProcessingStatus.QUEUED).fileLocation(s3Key)
                .build();
        return fileMasterAtomicService.attemptToCreate(potentialNewFile);
    }

    private void saveSkippedDuplicateFile(ProcessingJob job, Integer gxBucketId, @Nullable ZipMaster zipMaster, FileMetadata metadata, Long duplicateOfId) {
        FileMaster skipped = FileMaster.builder()
                .processingJob(job)
                .gxBucketId(gxBucketId)
                .fileName(metadata.fileName())
                .fileSize(metadata.fileSize())
                .extension(metadata.extension())
                .fileHash(metadata.fileHash())
                .zipMaster(zipMaster)
                .sourceType(zipMaster == null ? SourceType.UPLOADED : SourceType.EXTRACTED)
                .fileProcessingStatus(FileProcessingStatus.DUPLICATE)
                .duplicateOfFileId(duplicateOfId)
                .fileLocation("N/A")
                .build();
        fileMasterRepository.save(skipped);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markAsCompletedIfStillInProgress(Long fileMasterId) {
        fileMasterRepository.findById(fileMasterId).ifPresent(fm -> {
            if (fm.getFileProcessingStatus() == FileProcessingStatus.IN_PROGRESS) {
                fm.setFileProcessingStatus(FileProcessingStatus.COMPLETED);
                fileMasterRepository.save(fm);
                log.info("Pipeline completed successfully for original FileMaster ID: {}.", fileMasterId);
            }
        });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateFileMasterStatus(Long fileMasterId, FileProcessingStatus status, @Nullable String errorMessage) {
        fileMasterRepository.findById(fileMasterId).ifPresent(fm -> {
            fm.setFileProcessingStatus(status);
            if (errorMessage != null) {
                fm.setErrorMessage(errorMessage);
            }
            fileMasterRepository.save(fm);
            log.debug("Live-updated status for FileMaster ID {} to {}", fileMasterId, status);
        });
    }

    private void saveIgnoredFileFromPipeline(ProcessingJob job, ExtractedFileItem item, String hash, SourceType sourceType) {
        FileMaster ignoredFile = FileMaster.builder().processingJob(job).gxBucketId(job.getGxBucketId())
                .fileName(item.getFilename()).fileSize((long) item.getContent().length)
                .extension(FilenameUtils.getExtension(item.getFilename()).toLowerCase())
                .fileHash(hash).sourceType(sourceType)
                .fileProcessingStatus(FileProcessingStatus.IGNORED).errorMessage("Validation failed")
                .fileLocation("N/A").build();
        fileMasterRepository.save(ignoredFile);
    }

    private boolean isJobTerminated(FileMaster fileMaster) {
        if (fileMaster.getProcessingJob().getStatus() == ProcessingStatus.TERMINATED) {
            log.warn("Skipping pipeline for FileMaster ID {}: Job was terminated.", fileMaster.getId());
            if (fileMaster.getFileProcessingStatus() != FileProcessingStatus.TERMINATED) {
                self.updateFileMasterStatus(fileMaster.getId(), FileProcessingStatus.TERMINATED, "Job terminated by user.");
            }
            return true;
        }
        return false;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Optional<FileMaster> processNewItemForQueueing(final ExtractedFileItem item,
                                                          final FileMaster parentFile,
                                                          final SourceType sourceType) {
        final FileMetadata metadata = buildMetadata(item.getFilename(), item.getContent());


        final ProcessingJob parentJob = parentFile.getProcessingJob();
        final Integer gxBucketId = parentJob.getGxBucketId();

        return handleValidationAndDuplication(
                metadata.fileName(),
                metadata.fileSize(),
                metadata.fileHash(),
                gxBucketId,
                () -> saveIgnoredFileFromPipeline(parentJob, item, metadata.fileHash(), sourceType),
                existingWinner -> {
                    saveSkippedDuplicateFile(parentJob, gxBucketId, parentFile.getZipMaster(), metadata, existingWinner.getId());
                    return Optional.empty();
                },
                () -> {
                    final String s3Key = S3StorageService.constructS3Key(metadata.fileName(), gxBucketId, parentJob.getId(), "files");
                    FileMaster newFile = createNewFileMaster(parentFile, sourceType, metadata, s3Key);
                    asyncTaskManager.scheduleUploadAfterCommit(
                            newFile.getId(), s3Key, item.getContent(), fileMasterPostUploadAction
                    );
                    return Optional.of(newFile);
                }
        );
    }


    private boolean prepareAndCheckForDirectUploadDuplicates(FileMaster fileMaster, Path tempFile) throws IOException {
        final String fileName = fileMaster.getFileName();
        // The GxBucketId is needed for lookups, so it can be retrieved early.
        final Integer gxBucketId = fileMaster.getGxBucketId();

        // 1. FIRST, download the file to compute its hash and get the actual size (bytesRead).
        final HashResult hashResult = downloadAndHashFile(fileMaster, tempFile);
        final String fileHash = hashResult.hexHash();
        // 2. Use the ACTUAL size from the download, not the potentially null one from the entity.
        final long fileSize = hashResult.bytesRead();

        // 3. NOW, proceed with validation and duplicate checking using the correct values.
        Optional<Boolean> result = handleValidationAndDuplication(
                fileName,
                fileSize, // Use the just-calculated size
                fileHash,
                gxBucketId,
                () -> updateFileStatusToIgnored(fileMaster, "Validation failed"),
                existingWinner -> {
                    self.updateStatusToDuplicate(fileMaster.getId(), existingWinner.getId(), fileHash);
                    return Optional.of(false);
                },
                () -> {
                    try {
                        // Update the entity with the correct hash and size
                        self.updateFileMasterHashAndSize(fileMaster.getId(), fileHash, fileSize);
                        fileMaster.setFileHash(fileHash);
                        fileMaster.setFileSize(fileSize);
                        return Optional.of(true);
                    } catch (DataIntegrityViolationException e) {
                        log.warn("Race condition detected for hash '{}' on FileMaster ID {}.", fileHash, fileMaster.getId());
                        FileMaster winner = fileMasterAtomicService.findWinner(gxBucketId, fileHash)
                                .orElseThrow(() -> new IllegalStateException("FATAL: Race condition occurred, but winner not found for hash " + fileHash, e));
                        self.updateStatusToDuplicate(fileMaster.getId(), winner.getId(), fileHash);
                        return Optional.of(false);
                    }
                }
        );

        return result.orElse(false);
    }


    /**
     * Centralized shared logic for validation and duplicate detection.
     */
    private <T> Optional<T> handleValidationAndDuplication(
            String fileName,
            long fileSize,
            String fileHash,
            Integer gxBucketId,
            Runnable onValidationFailed,
            Function<FileMaster, Optional<T>> onDuplicateDetected,
            Supplier<Optional<T>> onNewFileHandler
    ) {
        String validationError = validationService.validateFileFully(fileName, fileSize);
        if (validationError != null) {
            log.warn("Validation failed for file '{}': {}.", fileName, validationError);
            onValidationFailed.run();
            return Optional.empty();
        }

        Optional<FileMaster> existingWinner = fileMasterAtomicService.findWinner(gxBucketId, fileHash);
        if (existingWinner.isPresent()) {
            log.warn("Duplicate detected for file '{}'.", fileName);
            return onDuplicateDetected.apply(existingWinner.get());
        }

        try {
            return onNewFileHandler.get();
        } catch (DataIntegrityViolationException ex) {
            log.warn("Race condition: Another thread created file with hash '{}'.", fileHash);
            FileMaster winner = fileMasterAtomicService.findWinner(gxBucketId, fileHash)
                    .orElseThrow(() -> new IllegalStateException("FATAL: Race condition occurred, but winner not found for hash " + fileHash, ex));
            return onDuplicateDetected.apply(winner);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateFileMasterHashAndSize(Long fileMasterId, String hash, long size) {
        fileMasterRepository.findById(fileMasterId).ifPresent(fm -> {
            fm.setFileHash(hash);
            fm.setFileSize(size);
            fileMasterRepository.save(fm);
        });
    }

    private List<ExtractedFileItem> findAndExecuteHandler(FileMaster fileMaster, Path tempFile) throws IOException, OfficeException {
        Optional<FileHandler> handlerOpt = fileHandlerFactory.getHandler(fileMaster.getExtension());
        if (handlerOpt.isEmpty()) {
            updateFileStatusToIgnored(fileMaster, "File type '" + fileMaster.getExtension() + "' is not supported.");
            return Collections.emptyList();
        }
        try (InputStream inputStream = Files.newInputStream(tempFile)) {
            return handlerOpt.get().handle(new BufferedInputStream(inputStream), fileMaster);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void createOrUpdateGxMasterRecord(final FileMaster sourceFile, @Nullable final ExtractedFileItem transformedContent) {
        if (sourceFile.getFileProcessingStatus() == FileProcessingStatus.DUPLICATE) {
            log.debug("Skipping GxMaster creation for DUPLICATE FileMaster ID: {}", sourceFile.getId());
            return;
        }

        // This method's logic is "find and update, or create if not found".
        final GxMaster gxRecord = gxMasterRepository.findBySourceFileId(sourceFile.getId())
                .orElseGet(() -> GxMaster.builder().sourceFile(sourceFile).build());

        populateAndScheduleUploadForGxMaster(gxRecord, sourceFile, transformedContent);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void createGxMasterForFinalArtifact(final ExtractedFileItem finalArtifact, final FileMaster sourceFile) {
        log.info("Creating new GxMaster record for artifact '{}' from source FileMaster ID: {}", finalArtifact.getFilename(), sourceFile.getId());

        // This method's logic is "always create new".
        final GxMaster gxRecord = GxMaster.builder().sourceFile(sourceFile).build();

        populateAndScheduleUploadForGxMaster(gxRecord, sourceFile, finalArtifact);
    }

    /**
     * Populates a GxMaster record and handles either new uploads or existing file copies.
     */
    private void populateAndScheduleUploadForGxMaster(
            GxMaster gxRecord, FileMaster sourceFile, @Nullable ExtractedFileItem artifact) {

        final ProcessingJob job = sourceFile.getProcessingJob();
        final boolean isNewContent = (artifact != null);
        final String finalS3Key;

        if (isNewContent) {
            finalS3Key = S3StorageService.constructS3Key(
                    artifact.getFilename(), sourceFile.getGxBucketId(), job.getId(), "files"
            );
            gxRecord.setProcessedFileName(artifact.getFilename());
            gxRecord.setFileSize((long) artifact.getContent().length);
            gxRecord.setExtension(FilenameUtils.getExtension(artifact.getFilename()).toLowerCase());
        } else {
            finalS3Key = s3StorageService.copyToGxFiles(
                    sourceFile.getFileLocation(), sourceFile.getFileName(),
                    sourceFile.getGxBucketId(), job.getId()
            );
            gxRecord.setProcessedFileName(sourceFile.getFileName());
            gxRecord.setFileSize(sourceFile.getFileSize());
            gxRecord.setExtension(sourceFile.getExtension());
        }

        // Common fields
        gxRecord.setFileLocation(finalS3Key);
        gxRecord.setGxBucketId(sourceFile.getGxBucketId());
        gxRecord.setGxStatus(job.isSkipGxProcess() ? GxStatus.SKIPPED : GxStatus.QUEUED_FOR_UPLOAD);
        gxRecord.setGxProcessId(job.isSkipGxProcess() ? NIL_UUID : null);

        // Single save point
        GxMaster savedRecord = gxMasterRepository.save(gxRecord);
        log.info("{} GxMaster record ID {} for source FileMaster ID: {}.",
                (gxRecord.getId() == null ? "Created" : "Updated"), savedRecord.getId(), sourceFile.getId());

        // Schedule async upload only for new content
        if (isNewContent) {
            asyncTaskManager.scheduleUploadAfterCommit(
                    savedRecord.getId(), finalS3Key, artifact.getContent(), gxMasterPostUploadAction
            );
        }
    }

    private HashResult downloadAndHashFile(FileMaster fileMaster, Path tempFile) throws IOException {
        try {
            final MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            long bytesRead;
            try (InputStream s3Stream = s3StorageService.downloadStream(fileMaster.getFileLocation());
                 BufferedInputStream buffered = new BufferedInputStream(s3Stream);
                 DigestInputStream digestStream = new DigestInputStream(buffered, sha256)) {
                bytesRead = Files.copy(digestStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
            }
            return new HashResult(Hex.encodeHexString(sha256.digest()), bytesRead);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }

    private void cleanupTempFile(Path tempFile) {
        if (tempFile != null) {
            try {
                Files.deleteIfExists(tempFile);
            } catch (IOException e) {
                log.error("Failed to delete temp file: {}", tempFile, e);
            }
        }
    }

    private SourceType determineSourceType(final FileMaster originalFile) {
        return "msg".equalsIgnoreCase(originalFile.getExtension()) ? SourceType.EXTRACTED : SourceType.TRANSFORMED;
    }

    private void updateFileStatusToIgnored(final FileMaster fileMaster, final String reason) {
        self.updateFileMasterStatus(fileMaster.getId(), FileProcessingStatus.IGNORED, reason);
        fileMaster.setFileProcessingStatus(FileProcessingStatus.IGNORED);
        fileMaster.setErrorMessage(reason);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateStatusToDuplicate(final Long fileMasterId, final Long duplicateOfId, String hash) {
        fileMasterRepository.findById(fileMasterId).ifPresent(fm -> {
            fm.setFileProcessingStatus(FileProcessingStatus.DUPLICATE);
            fm.setDuplicateOfFileId(duplicateOfId);
            fm.setFileHash(hash);
            fileMasterRepository.save(fm);
        });
    }

    private void handlePipelineFailure(Long fileMasterId, Exception e) {
        log.error("A critical exception aborted the pipeline for FileMaster ID: {}.", fileMasterId, e);
        self.updateFileMasterStatus(fileMasterId, FileProcessingStatus.FAILED, e.getMessage());
        jobLifecycleManager.failJobForFileProcessing(fileMasterId, e.getMessage());
        throw new MessageProcessingFailedException("Pipeline failed for FileMaster ID " + fileMasterId, e);
    }

    private FileMetadata buildMetadata(String fileName, byte[] content) {
        return new FileMetadata(
                fileName,
                content.length,
                FilenameUtils.getExtension(fileName).toLowerCase(),
                DigestUtils.sha256Hex(content)
        );
    }

    private boolean isExtractionRequired(FileMaster sourceFile, List<ExtractedFileItem> results) {
        return results.size() > 1 || (results.size() == 1 && !results.getFirst().getFilename().equals(sourceFile.getFileName()));
    }

    private record HashResult(String hexHash, long bytesRead) {
    }
}
