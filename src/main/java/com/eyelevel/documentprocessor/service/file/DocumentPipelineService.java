package com.eyelevel.documentprocessor.service.file;

import com.eyelevel.documentprocessor.exception.MessageProcessingFailedException;
import com.eyelevel.documentprocessor.model.*;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.GxMasterRepository;
import com.eyelevel.documentprocessor.service.handlers.FileHandler;
import com.eyelevel.documentprocessor.service.handlers.factory.FileHandlerFactory;
import com.eyelevel.documentprocessor.service.lifecycle.JobLifecycleManager;
import com.eyelevel.documentprocessor.service.s3.S3StorageService;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.jodconverter.core.office.OfficeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

@Slf4j
@Service
public class DocumentPipelineService {

    // Service and repository dependencies
    private final FileMasterRepository fileMasterRepository;
    private final GxMasterRepository gxMasterRepository;
    private final FileHandlerFactory fileHandlerFactory;
    private final S3StorageService s3StorageService;
    private final SqsTemplate sqsTemplate;
    private final ValidationService validationService;
    private final JobLifecycleManager jobLifecycleManager;
    private final FileMasterAtomicService fileMasterAtomicService;

    @Value("${aws.sqs.file-queue-name}")
    private String fileQueueName;

    private DocumentPipelineService self;

    private static final UUID NIL_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");
    private static final String SQS_MESSAGE_GROUP_ID_HEADER = "message-group-id";
    private static final String SQS_MESSAGE_DEDUPLICATION_ID_HEADER = "message-deduplication-id";

    public DocumentPipelineService(FileMasterRepository fmr, GxMasterRepository gmr, FileHandlerFactory fhf,
                                   S3StorageService s3, SqsTemplate sqs, ValidationService vs, JobLifecycleManager jlm,
                                   FileMasterAtomicService fmas) {
        this.fileMasterRepository = fmr;
        this.gxMasterRepository = gmr;
        this.fileHandlerFactory = fhf;
        this.s3StorageService = s3;
        this.sqsTemplate = sqs;
        this.validationService = vs;
        this.jobLifecycleManager = jlm;
        this.fileMasterAtomicService = fmas;
    }

    @Autowired
    public void setSelf(@Lazy DocumentPipelineService self) {
        this.self = self;
    }

    /**
     * MODIFICATION: New method to provide live status updates.
     * Updates the status of a FileMaster record in a new, independent transaction.
     * This ensures the status change is committed and visible immediately.
     *
     * @param fileMasterId The ID of the FileMaster to update.
     * @param status       The new FileProcessingStatus to set.
     * @param errorMessage An optional error message for failure states.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateFileMasterStatus(Long fileMasterId, FileProcessingStatus status, @Nullable String errorMessage) {
        FileMaster fileMaster = fileMasterRepository.findById(fileMasterId).orElseThrow(
                () -> new IllegalStateException("Cannot update status. FileMaster not found: " + fileMasterId));

        fileMaster.setFileProcessingStatus(status);
        if (errorMessage != null) {
            fileMaster.setErrorMessage(errorMessage);
        }
        fileMasterRepository.save(fileMaster);
        log.debug("Live-updated status for FileMaster ID {} to {}", fileMasterId, status);
    }

    /**
     * MODIFIED: Orchestrates the processing pipeline, now with live status updates.
     *
     * @param fileMasterId The ID of the {@link FileMaster} to process.
     */
    @Transactional
    public void runPipeline(final Long fileMasterId) {
        log.info("Beginning document pipeline for FileMaster ID: {}", fileMasterId);

        // MODIFICATION: Set status to IN_PROGRESS immediately so it's visible to the user.
        self.updateFileMasterStatus(fileMasterId, FileProcessingStatus.IN_PROGRESS, null);

        final FileMaster fileMaster = fileMasterRepository.findById(fileMasterId).orElseThrow(
                () -> new IllegalStateException("Not found: " + fileMasterId));

        Path tempFile = null;
        try {
            tempFile = Files.createTempFile("pipeline-" + fileMasterId + "-", fileMaster.getFileName());

            if (!prepareAndCheckForDirectUploadDuplicates(fileMaster, tempFile)) {
                // The helper methods (updateFileStatusToIgnored/Skipped) now handle their own live updates.
                return;
            }

            final List<ExtractedFileItem> newItems = findAndExecuteHandler(fileMaster, tempFile);
            processHandlerResults(newItems, fileMaster);

            // MODIFICATION: Check the latest status from the DB before updating to COMPLETED.
            // This prevents overwriting a terminal state set by a sub-process.
            FileMaster currentFileMaster = fileMasterRepository.findById(fileMasterId).orElseThrow(
                    () -> new IllegalStateException(
                            "Cannot find FileMaster " + fileMasterId + " for final status check."));

            if (currentFileMaster.getFileProcessingStatus() == FileProcessingStatus.IN_PROGRESS) {
                // Update status to COMPLETED using the live-update method.
                self.updateFileMasterStatus(fileMasterId, FileProcessingStatus.COMPLETED, null);
                log.info("Pipeline completed for original FileMaster ID: {}.", fileMasterId);
            }
        } catch (final Exception e) {
            log.error("A critical exception aborted pipeline for FileMaster ID: {}.", fileMasterId, e);
            // MODIFICATION: Set status to FAILED immediately so it's visible to the user.
            self.updateFileMasterStatus(fileMasterId, FileProcessingStatus.FAILED, e.getMessage());
            jobLifecycleManager.failJobForFileProcessing(fileMasterId, e.getMessage());
            throw new MessageProcessingFailedException("Pipeline failed for FileMaster ID " + fileMasterId, e);
        } finally {
            cleanupTempFile(tempFile);
        }
    }

    private boolean prepareAndCheckForDirectUploadDuplicates(FileMaster fileMaster, Path tempFile) throws IOException {
        final HashResult hashResult = downloadAndHashFile(fileMaster, tempFile);
        fileMaster.setFileSize(hashResult.bytesRead());

        final String validationError = validationService.validateFileFully(fileMaster.getFileName(),
                                                                           fileMaster.getFileSize());
        if (validationError != null) {
            updateFileStatusToIgnored(fileMaster, validationError, fileMaster.getFileSize());
            return false;
        }

        if (fileMaster.getFileHash() == null) {
            final String hash = hashResult.hexHash();
            log.info("Calculated initial hash for direct upload FileMaster ID {}: {}", fileMaster.getId(), hash);

            final Optional<FileMaster> existingDuplicate = fileMasterRepository.findFirstByGxBucketIdAndFileHashAndIdNotAndFileProcessingStatusNotInOrderByIdAsc(
                    fileMaster.getGxBucketId(), hash, fileMaster.getId(),
                    List.of(FileProcessingStatus.FAILED, FileProcessingStatus.IGNORED));

            if (existingDuplicate.isPresent()) {
                log.warn("Direct upload FileMaster ID {} is a duplicate of {}. Marking as DUPLICATE.",
                         fileMaster.getId(), existingDuplicate.get().getId());
                fileMaster.setFileHash(hash);
                updateFileStatusToSkipped(fileMaster, existingDuplicate.get().getId());
                return false;
            } else {
                fileMaster.setFileHash(hash);
            }
        }
        return true;
    }

    private List<ExtractedFileItem> findAndExecuteHandler(FileMaster fileMaster, Path tempFile)
    throws IOException, OfficeException {
        final Optional<FileHandler> handlerOpt = fileHandlerFactory.getHandler(fileMaster.getExtension());

        if (handlerOpt.isEmpty()) {
            final String error = String.format("File type '%s' is not supported.", fileMaster.getExtension());
            updateFileStatusToIgnored(fileMaster, error, fileMaster.getFileSize());
            return Collections.emptyList();
        }
        return handlerOpt.get().handle(Files.newInputStream(tempFile), fileMaster);
    }

    private void processHandlerResults(final List<ExtractedFileItem> newItems, final FileMaster fileMaster) {
        if (newItems.isEmpty() ||
            (newItems.size() == 1 && newItems.getFirst().getFilename().equals(fileMaster.getFileName()))) {
            final ExtractedFileItem transformedItem = newItems.isEmpty() ? null : newItems.getFirst();
            createOrUpdateGxMasterRecord(fileMaster, transformedItem);
            return;
        }

        if (fileMaster.getExtension().equalsIgnoreCase("pdf")) {
            log.info("PdfHandler produced {} final artifact(s). Creating GxMaster records directly in parallel.",
                     newItems.size());
            newItems.parallelStream().forEach(item -> {
                try {
                    self.createGxMasterForFinalArtifactInNewTransaction(fileMaster, item);
                } catch (Exception e) {
                    log.error(
                            "Failed to create GxMaster for final artifact '{}' from source FileMaster ID {}. The pipeline for other artifacts will continue.",
                            item.getFilename(), fileMaster.getId(), e);
                }
            });
        } else {
            log.info("Handler created {} new source file(s) from FileMaster ID {}. Queueing for individual processing.",
                     newItems.size(), fileMaster.getId());
            final SourceType sourceType = determineSourceType(fileMaster);
            queueNewItemsForProcessing(newItems, fileMaster.getProcessingJob(), sourceType);
        }
    }

    private void queueNewItemsForProcessing(final List<ExtractedFileItem> items, final ProcessingJob parentJob,
                                            final SourceType sourceType) {
        final List<FileMaster> newFilesToProcess = items.stream()
                                                        .map(item -> self.processNewItemForQueueing(item, parentJob,
                                                                                                    sourceType))
                                                        .filter(Optional::isPresent).map(Optional::get).toList();

        if (newFilesToProcess.isEmpty()) {
            log.info("All new items were duplicates or failed to process. No new files to queue.");
            return;
        }

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                log.info("Queueing {} new FileMasters for processing.", newFilesToProcess.size());
                newFilesToProcess.forEach(file -> {
                    final String groupId = String.valueOf(file.getGxBucketId());
                    final String dedupeId = groupId + "-" + file.getFileHash();
                    sqsTemplate.send(fileQueueName, MessageBuilder.withPayload(Map.of("fileMasterId", file.getId()))
                                                                  .setHeader(SQS_MESSAGE_GROUP_ID_HEADER, groupId)
                                                                  .setHeader(SQS_MESSAGE_DEDUPLICATION_ID_HEADER,
                                                                             dedupeId).build());
                });
            }
        });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Optional<FileMaster> processNewItemForQueueing(final ExtractedFileItem item, final ProcessingJob parentJob,
                                                          final SourceType sourceType) {
        final long fileSize = item.getContent().length;
        final String validationError = validationService.validateFileFully(item.getFilename(), fileSize);
        final String hash = DigestUtils.sha256Hex(item.getContent());
        final String extension = FilenameUtils.getExtension(item.getFilename()).toLowerCase();
        if (validationError != null) {
            log.warn("Newly extracted item '{}' failed validation: {}. Marking as IGNORED.", item.getFilename(),
                     validationError);
            FileMaster ignoredFile = FileMaster.builder().processingJob(parentJob).gxBucketId(parentJob.getGxBucketId())
                                               .fileName(item.getFilename()).fileSize(fileSize).extension(extension)
                                               .fileHash(hash).sourceType(sourceType)
                                               .fileProcessingStatus(FileProcessingStatus.IGNORED)
                                               .errorMessage(validationError).fileLocation("N/A").build();
            fileMasterRepository.save(ignoredFile);
            return Optional.empty();
        }

        final Integer gxBucketId = parentJob.getGxBucketId();
        final Optional<FileMaster> existingWinner = fileMasterAtomicService.findWinner(gxBucketId, hash);
        if (existingWinner.isPresent()) {
            log.warn("Duplicate content for newly created item '{}'. Creating new duplicate record.",
                     item.getFilename());
            self.saveSkippedDuplicateFileFromPipeline(parentJob, item, existingWinner.get().getId(), hash, sourceType);
            return Optional.empty();
        }

        final String s3Key = S3StorageService.constructS3Key(item.getFilename(), gxBucketId, parentJob.getId(),
                                                             "files");
        FileMaster potentialNewFile = FileMaster.builder().processingJob(parentJob).gxBucketId(gxBucketId)
                                                .fileName(item.getFilename()).fileSize(fileSize).extension(extension)
                                                .fileHash(hash).sourceType(sourceType)
                                                .fileProcessingStatus(FileProcessingStatus.QUEUED).fileLocation(s3Key)
                                                .build();

        try {
            FileMaster newFile = fileMasterAtomicService.attemptToCreate(potentialNewFile);
            s3StorageService.upload(s3Key, new ByteArrayInputStream(item.getContent()), fileSize);
            return Optional.of(newFile);
        } catch (DataIntegrityViolationException ex) {
            log.warn(
                    "Race condition detected for hash '{}' while processing item '{}'. Finding winner and creating duplicate record.",
                    hash, item.getFilename());
            FileMaster winner = fileMasterAtomicService.findWinner(gxBucketId, hash).orElseThrow(
                    () -> new IllegalStateException(
                            "FATAL: Race condition occurred, but winner not found for hash " + hash, ex));
            self.saveSkippedDuplicateFileFromPipeline(parentJob, item, winner.getId(), hash, sourceType);
            return Optional.empty();
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveSkippedDuplicateFileFromPipeline(ProcessingJob job, ExtractedFileItem item, Long duplicateOfId,
                                                     String hash, SourceType sourceType) {
        FileMaster skipped = FileMaster.builder().processingJob(job).gxBucketId(job.getGxBucketId())
                                       .fileName(item.getFilename()).fileSize((long) item.getContent().length)
                                       .extension(FilenameUtils.getExtension(item.getFilename()).toLowerCase())
                                       .fileHash(hash).sourceType(sourceType)
                                       .fileProcessingStatus(FileProcessingStatus.DUPLICATE)
                                       .duplicateOfFileId(duplicateOfId).fileLocation("N/A").build();
        fileMasterRepository.save(skipped);
    }

    private void createOrUpdateGxMasterRecord(final FileMaster sourceFile,
                                              @Nullable final ExtractedFileItem transformedContent) {
        if (sourceFile.getFileProcessingStatus() == FileProcessingStatus.DUPLICATE) {
            log.debug("Skipping GxMaster creation for DUPLICATE FileMaster ID: {}", sourceFile.getId());
            return;
        }

        final String processedFileName;
        final long finalSize;
        final String finalS3Key;

        if (transformedContent != null) {
            final byte[] content = transformedContent.getContent();
            processedFileName = transformedContent.getFilename();
            finalSize = content.length;
            finalS3Key = S3StorageService.constructS3Key(processedFileName, sourceFile.getGxBucketId(),
                                                         sourceFile.getProcessingJob().getId(), "files");
            s3StorageService.upload(finalS3Key, new ByteArrayInputStream(content), finalSize);
        } else {
            processedFileName = sourceFile.getFileName();
            finalSize = sourceFile.getFileSize();
            finalS3Key = sourceFile.getFileLocation();
        }

        final ProcessingJob job = sourceFile.getProcessingJob();
        final GxStatus status = job.isSkipGxProcess() ? GxStatus.SKIPPED : GxStatus.QUEUED_FOR_UPLOAD;
        final UUID processId = job.isSkipGxProcess() ? NIL_UUID : null;

        final GxMaster gxRecord = gxMasterRepository.findBySourceFileId(sourceFile.getId())
                                                    .orElseGet(() -> GxMaster.builder().sourceFile(sourceFile).build());

        gxRecord.setGxBucketId(sourceFile.getGxBucketId());
        gxRecord.setFileLocation(
                s3StorageService.copyToGxFiles(finalS3Key, processedFileName, sourceFile.getGxBucketId(), job.getId()));
        gxRecord.setProcessedFileName(processedFileName);
        gxRecord.setFileSize(finalSize);
        gxRecord.setExtension(FilenameUtils.getExtension(processedFileName).toLowerCase());
        gxRecord.setGxStatus(status);
        gxRecord.setGxProcessId(processId);

        gxMasterRepository.save(gxRecord);
        log.info("{} GxMaster record for FileMaster ID: {}", gxRecord.getId() == null ? "Created" : "Updated",
                 sourceFile.getId());
    }

    private record HashResult(long bytesRead, String hexHash) {
    }

    private HashResult downloadAndHashFile(FileMaster fileMaster, Path tempFile) throws IOException {
        try {
            final MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            long bytesRead;
            try (InputStream s3Stream = s3StorageService.downloadStream(fileMaster.getFileLocation());
                 DigestInputStream digestStream = new DigestInputStream(s3Stream, sha256)) {
                bytesRead = Files.copy(digestStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
            }
            final String hexHash = Hex.encodeHexString(sha256.digest());
            return new HashResult(bytesRead, hexHash);
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

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void createGxMasterForFinalArtifactInNewTransaction(final FileMaster sourceFile,
                                                               final ExtractedFileItem finalArtifact) {
        final byte[] content = finalArtifact.getContent();
        final ProcessingJob parentJob = sourceFile.getProcessingJob();
        final String s3Key = S3StorageService.constructS3Key(finalArtifact.getFilename(), sourceFile.getGxBucketId(),
                                                             parentJob.getId(), "files");
        s3StorageService.upload(s3Key, new ByteArrayInputStream(content), content.length);

        final GxStatus status = parentJob.isSkipGxProcess() ? GxStatus.SKIPPED : GxStatus.QUEUED_FOR_UPLOAD;
        final UUID processId = parentJob.isSkipGxProcess() ? NIL_UUID : null;
        final String gxFilesS3Key = s3StorageService.copyToGxFiles(s3Key, finalArtifact.getFilename(),
                                                                   sourceFile.getGxBucketId(), parentJob.getId());

        final GxMaster newRecord = GxMaster.builder().sourceFile(sourceFile).gxBucketId(sourceFile.getGxBucketId())
                                           .fileLocation(gxFilesS3Key).processedFileName(finalArtifact.getFilename())
                                           .fileSize((long) content.length).extension(
                        FilenameUtils.getExtension(finalArtifact.getFilename()).toLowerCase()).gxStatus(status)
                                           .gxProcessId(processId).build();
        gxMasterRepository.save(newRecord);
        log.info("Created GxMaster ID {} for final artifact '{}'", newRecord.getId(), finalArtifact.getFilename());
    }

    private SourceType determineSourceType(final FileMaster originalFile) {
        final String extension = originalFile.getExtension();
        if ("msg".equalsIgnoreCase(extension)) {
            return SourceType.EXTRACTED;
        }
        if (originalFile.getSourceType() == SourceType.UPLOADED) {
            return SourceType.TRANSFORMED;
        }
        return SourceType.EXTRACTED;
    }

    /**
     * MODIFIED: Helper method now calls the public, live-update method.
     */
    private void updateFileStatusToIgnored(final FileMaster fileMaster, final String reason, final long fileSize) {
        // Call the public method to make the change visible immediately.
        self.updateFileMasterStatus(fileMaster.getId(), FileProcessingStatus.IGNORED, reason);

        // Also update the in-memory object for any subsequent logic in the current transaction.
        fileMaster.setFileProcessingStatus(FileProcessingStatus.IGNORED);
        fileMaster.setErrorMessage(reason);
        fileMaster.setFileSize(fileSize);
    }

    /**
     * MODIFIED: Helper method now calls the public, live-update method.
     */
    private void updateFileStatusToSkipped(final FileMaster fileMaster, final Long duplicateOfId) {
        // Call the public method to make the change visible immediately.
        self.updateFileMasterStatus(fileMaster.getId(), FileProcessingStatus.DUPLICATE, null);

        // Also update the in-memory object for any subsequent logic in the current transaction.
        fileMaster.setFileProcessingStatus(FileProcessingStatus.DUPLICATE);
        fileMaster.setDuplicateOfFileId(duplicateOfId);
    }
}