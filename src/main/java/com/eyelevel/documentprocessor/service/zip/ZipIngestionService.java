package com.eyelevel.documentprocessor.service.zip;

import com.eyelevel.documentprocessor.common.apiclient.gx.GXApiClient;
import com.eyelevel.documentprocessor.dto.gx.creategxbucket.response.GXBucket;
import com.eyelevel.documentprocessor.exception.DocumentProcessingException;
import com.eyelevel.documentprocessor.exception.MessageProcessingFailedException;
import com.eyelevel.documentprocessor.model.*;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.ZipMasterRepository;
import com.eyelevel.documentprocessor.service.file.FileMasterAtomicService;
import com.eyelevel.documentprocessor.service.file.ValidationService;
import com.eyelevel.documentprocessor.service.s3.S3StorageService;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.zip.ZipException;

/**
 * Service responsible for orchestrating the ingestion of ZIP files.
 * This service manages the entire lifecycle of processing a ZIP archive, from downloading it from S3,
 * streaming its contents, processing each entry concurrently, and finally queuing valid files for further steps.
 * It uses a combination of virtual threads and a semaphore to control concurrency and manage resources effectively.
 */
@Slf4j
@Service
public class ZipIngestionService {

    // Configuration for large file processing
    @Value("${app.processing.zip-handler.concurrency-limit}")
    private int concurrencyLimit;

    @Value("${app.processing.zip-handler.temp-dir}")
    private String tempFileDir;

    @Value("${aws.sqs.file-queue-name}")
    private String fileQueueName;

    // Service dependencies
    private final ZipMasterRepository zipMasterRepository;
    private final S3StorageService s3StorageService;
    private final SqsTemplate sqsTemplate;
    private final ValidationService validationService;
    private final GXApiClient gxApiClient;
    private final ZipStreamProcessor zipStreamProcessor;
    private final FileMasterAtomicService fileMasterAtomicService;
    private final FileMasterRepository fileMasterRepository;
    private ZipIngestionService self; // Self-proxy for @Transactional calls

    // SQS constants
    private static final String SQS_MESSAGE_GROUP_ID_HEADER = "message-group-id";
    private static final String SQS_MESSAGE_DEDUPLICATION_ID_HEADER = "message-deduplication-id";

    /**
     * Constructs the service with its required dependencies.
     *
     * @param zmr  Repository for {@link ZipMaster} entities.
     * @param fmr  Repository for {@link FileMaster} entities.
     * @param s3   Service for interacting with S3.
     * @param sqs  Template for sending SQS messages.
     * @param vs   Service for file validation logic.
     * @param gxa  API client for interacting with the GX service.
     * @param zsp  The low-level ZIP stream processor.
     * @param fmas Atomic service for handling {@link FileMaster} operations.
     */
    public ZipIngestionService(ZipMasterRepository zmr, FileMasterRepository fmr, S3StorageService s3, SqsTemplate sqs,
                               ValidationService vs, GXApiClient gxa, ZipStreamProcessor zsp,
                               FileMasterAtomicService fmas) {
        this.zipMasterRepository = zmr;
        this.fileMasterRepository = fmr;
        this.s3StorageService = s3;
        this.sqsTemplate = sqs;
        this.validationService = vs;
        this.gxApiClient = gxa;
        this.zipStreamProcessor = zsp;
        this.fileMasterAtomicService = fmas;
    }

    /**
     * Injects a lazy self-proxy to enable internal calls to {@code @Transactional} methods.
     *
     * @param self A proxy of the current instance.
     */
    @Autowired
    public void setSelf(@Lazy ZipIngestionService self) {
        this.self = self;
    }

    /**
     * MODIFIED: Updates the status of a ZipMaster in a new, independent transaction.
     * This ensures the status change is committed to the database and visible immediately,
     * even if the calling process is part of a longer-running transaction.
     *
     * @param zipMasterId The ID of the ZipMaster to update.
     * @param status      The new ZipProcessingStatus to set.
     * @param errorMessage An optional error message to set (used for failure states).
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateZipStatus(Long zipMasterId, ZipProcessingStatus status, String errorMessage) {
        ZipMaster zipMaster = zipMasterRepository.findById(zipMasterId)
                                                 .orElseThrow(() -> new IllegalStateException("Cannot update status. ZipMaster not found: " + zipMasterId));
        zipMaster.setZipProcessingStatus(status);
        if (errorMessage != null) {
            zipMaster.setErrorMessage(errorMessage);
        }
        // This save is committed immediately when the method returns, making the status visible.
        zipMasterRepository.save(zipMaster);
    }

    /**
     * Orchestrates the entire process of ingesting a ZIP file and queuing its contents.
     * MODIFIED: This method now calls the new `updateZipStatus` method to provide real-time
     * status updates for the overall ZIP processing job.
     *
     * @param zipMasterId The ID of the {@link ZipMaster} entity to process.
     */
    @Transactional
    public void ingestAndQueueFiles(final Long zipMasterId) {
        // Find the ZipMaster entity or throw an exception if it doesn't exist.
        final ZipMaster zipMaster = zipMasterRepository.findById(zipMasterId).orElseThrow(
                () -> new IllegalStateException("ZipMaster not found with ID: " + zipMasterId));

        // Ensure the ZIP file is in a state that allows processing.
        if (zipMaster.getZipProcessingStatus() != ZipProcessingStatus.QUEUED_FOR_EXTRACTION) {
            log.warn("ZipMaster ID {} is not in a processable state ({}), skipping.", zipMasterId,
                     zipMaster.getZipProcessingStatus());
            return;
        }

        // Call the new method to update the status in its own transaction.
        // This makes the 'EXTRACTION_IN_PROGRESS' status visible in the UI immediately.
        self.updateZipStatus(zipMasterId, ZipProcessingStatus.EXTRACTION_IN_PROGRESS, null);
        log.info("Beginning ingestion for ZipMaster ID: {}. Concurrency limit: {}, Temp dir: {}", zipMasterId,
                 concurrencyLimit, tempFileDir);

        try {
            // Create the temporary directory if it does not already exist.
            Files.createDirectories(Paths.get(tempFileDir));
        } catch (IOException e) {
            throw new DocumentProcessingException("Failed to create temporary directory: " + tempFileDir, e);
        }

        final Semaphore semaphore = new Semaphore(concurrencyLimit);
        final Map<String, GXBucket> bucketCache = new ConcurrentHashMap<>();

        final ProcessingJob processingJob = zipMaster.getProcessingJob();
        final Long processingJobId = processingJob.getId();
        final boolean isBulkUpload = processingJob.isBulkUpload();
        final Integer nonBulkGxBucketId = zipMaster.getGxBucketId();

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            try (InputStream s3InputStream = s3StorageService.downloadStream(zipMaster.getOriginalFilePath())) {

                final Consumer<ZipStreamProcessor.ZipEntryWorkItem> workItemConsumer = (item) -> {
                    try {
                        semaphore.acquire();
                        log.info("Processing entry: {}, Size: {}", item.normalizedPath(), item.fileSize());

                        executor.submit(() -> {
                            try {
                                final Integer gxBucketId = isBulkUpload ? getBucketIdForBulkUpload(
                                        item.normalizedPath(), bucketCache) : nonBulkGxBucketId;

                                if (gxBucketId != null) {
                                    self.processZipEntry(item, processingJobId, gxBucketId, zipMasterId);
                                }
                            } catch (Exception e) {
                                log.error("Unhandled exception processing entry '{}' in ZipMaster ID {}.",
                                          item.normalizedPath(), zipMaster.getId(), e);
                            } finally {
                                semaphore.release();
                            }
                        });
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("Interrupted while waiting for semaphore permit.", e);
                        throw new RuntimeException(e);
                    }
                };

                zipStreamProcessor.processStream(s3InputStream, Paths.get(tempFileDir), workItemConsumer);

                // Update final status to SUCCESS using the transactional method.
                self.updateZipStatus(zipMasterId, ZipProcessingStatus.EXTRACTION_SUCCESS, null);
                log.info("Successfully completed ingestion for ZipMaster ID: {}", zipMasterId);

            } catch (Exception e) {
                // If any exception occurs, update the status to FAILED using the transactional method.
                String errorMsg = "A processing error occurred during extraction: " + e.getMessage();
                self.updateZipStatus(zipMasterId, ZipProcessingStatus.EXTRACTION_FAILED, errorMsg);
                log.error("Ingestion failed for ZipMaster ID {}: A processing error occurred.", zipMasterId, e);

                if (!(e instanceof ZipException)) {
                    throw new MessageProcessingFailedException("Failed to process ZIP for ZipMaster ID " + zipMasterId,
                                                               e);
                }
            }
        }
        // The final save in a 'finally' block is no longer needed as status updates are handled transactionally.
    }

    /**
     * Processes a single entry from a ZIP file in a new transaction.
     * This method's logic remains unchanged, as it was already correctly designed for live updates.
     *
     * @param workItem        The {@link ZipStreamProcessor.ZipEntryWorkItem} containing metadata about the extracted file.
     * @param processingJobId The ID of the parent {@link ProcessingJob}.
     * @param gxBucketId      The ID of the GX Bucket this file belongs to.
     * @param zipMasterId     The ID of the parent {@link ZipMaster}.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processZipEntry(ZipStreamProcessor.ZipEntryWorkItem workItem, Long processingJobId, Integer gxBucketId,
                                Long zipMasterId) {
        final ZipMaster zipMaster = zipMasterRepository.findById(zipMasterId).orElseThrow(
                () -> new IllegalStateException("ZipMaster not found: " + zipMasterId));
        final ProcessingJob job = zipMaster.getProcessingJob();

        final Path tempFile = workItem.tempFilePath();
        try {
            final String fileName = FilenameUtils.getName(workItem.normalizedPath());
            final String extension = FilenameUtils.getExtension(fileName).toLowerCase();
            final long fileSize = workItem.fileSize();
            final String fileHash = workItem.sha256Hash();

            final String validationError = validationService.validateFileFully(fileName, fileSize);
            if (validationError != null) {
                self.saveIgnoredFile(job, gxBucketId, zipMaster, fileName, fileSize, extension, validationError);
                return;
            }

            final Optional<FileMaster> existingWinner = fileMasterAtomicService.findWinner(gxBucketId, fileHash);
            if (existingWinner.isPresent()) {
                handleDuplicate(existingWinner.get(), job, gxBucketId, zipMaster, fileName, fileSize, extension,
                                fileHash);
                return;
            }

            final String s3Key = S3StorageService.constructS3Key(fileName, gxBucketId, job.getId(), "files");
            FileMaster potentialNewFile = buildFileMaster(job, gxBucketId, zipMaster, fileName, fileSize, extension,
                                                          fileHash, s3Key);

            try {
                FileMaster newFile = fileMasterAtomicService.attemptToCreate(potentialNewFile);
                handleNewFile(newFile, tempFile);
            } catch (DataIntegrityViolationException ex) {
                log.warn("Race condition detected for hash '{}'. Recovering by finding winner.", fileHash);
                FileMaster winner = fileMasterAtomicService.findWinner(gxBucketId, fileHash).orElseThrow(
                        () -> new IllegalStateException(
                                "FATAL: Race condition occurred, but winner not found for hash " + fileHash, ex));
                handleDuplicate(winner, job, gxBucketId, zipMaster, fileName, fileSize, extension, fileHash);
            }
        } catch (Exception e) {
            throw new DocumentProcessingException("Failed to process entry: " + workItem.normalizedPath(), e);
        } finally {
            try {
                Files.deleteIfExists(tempFile);
            } catch (IOException e) {
                log.error("CRITICAL: Failed to delete temporary file: {}", tempFile, e);
            }
        }
    }

    // --- The rest of the methods remain unchanged ---

    private Integer getBucketIdForBulkUpload(String entryName, Map<String, GXBucket> bucketCache) {
        final int separator = entryName.indexOf('/');
        if (separator == -1) {
            log.warn("[BULK] Skipping root-level file '{}'", entryName);
            return null;
        }
        final String bucketName = entryName.substring(0, separator);
        if (bucketName.isBlank() || bucketName.startsWith(".")) {
            log.warn("[BULK] Skipping hidden/blank bucket for file '{}'", entryName);
            return null;
        }
        return bucketCache.computeIfAbsent(bucketName, gxApiClient::createGXBucket).bucket().bucketId();
    }

    private FileMaster buildFileMaster(ProcessingJob job, Integer gxBucketId, ZipMaster zipMaster, String fileName,
                                       long fileSize, String extension, String fileHash, String s3Key) {
        return FileMaster.builder().processingJob(job).gxBucketId(gxBucketId).fileName(fileName).fileSize(fileSize)
                         .extension(extension).fileHash(fileHash).sourceType(SourceType.UPLOADED).zipMaster(zipMaster)
                         .fileProcessingStatus(FileProcessingStatus.QUEUED).fileLocation(s3Key).build();
    }

    private void handleNewFile(FileMaster newFile, Path tempFile) throws IOException {
        log.info("Successfully created new FileMaster ID: {} for hash '{}'. Uploading to S3.", newFile.getId(),
                 newFile.getFileHash());
        try (InputStream fis = Files.newInputStream(tempFile)) {
            s3StorageService.upload(newFile.getFileLocation(), fis, newFile.getFileSize());
        }
        self.queueFileForProcessing(newFile.getId(), newFile.getGxBucketId(), newFile.getFileHash());
    }

    private void handleDuplicate(FileMaster existingDuplicate, ProcessingJob job, Integer gxBucketId,
                                 ZipMaster zipMaster, String fileName, long fileSize, String extension,
                                 String fileHash) {
        log.warn(
                "DUPLICATE DETECTED for file '{}'. It is a duplicate of FileMaster ID {}. Creating new duplicate record.",
                fileName, existingDuplicate.getId());
        self.saveSkippedDuplicateFile(job, gxBucketId, zipMaster, fileName, fileSize, extension, fileHash,
                                      existingDuplicate.getId());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void queueFileForProcessing(Long fileMasterId, Integer gxBucketId, String fileHash) {
        log.info("Queueing new unique FileMaster ID: {} for processing.", fileMasterId);
        final String groupId = String.valueOf(gxBucketId);
        final String dedupeId = groupId + "-" + fileHash;
        sqsTemplate.send(fileQueueName, MessageBuilder.withPayload(Map.of("fileMasterId", fileMasterId))
                                                      .setHeader(SQS_MESSAGE_GROUP_ID_HEADER, groupId)
                                                      .setHeader(SQS_MESSAGE_DEDUPLICATION_ID_HEADER, dedupeId)
                                                      .build());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveIgnoredFile(ProcessingJob job, Integer gxBucketId, ZipMaster zipMaster, String fileName,
                                long fileSize, String extension, String errorMessage) {
        FileMaster ignored = FileMaster.builder().processingJob(job).gxBucketId(gxBucketId).fileName(fileName)
                                       .fileSize(fileSize).extension(extension)
                                       .fileProcessingStatus(FileProcessingStatus.IGNORED)
                                       .sourceType(SourceType.UPLOADED).zipMaster(zipMaster).errorMessage(errorMessage)
                                       .fileLocation("N/A").build();
        fileMasterRepository.save(ignored);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveSkippedDuplicateFile(ProcessingJob job, Integer gxBucketId, ZipMaster zipMaster, String fileName,
                                         long fileSize, String extension, String fileHash, Long duplicateOfId) {
        FileMaster skipped = FileMaster.builder().processingJob(job).gxBucketId(gxBucketId).fileName(fileName)
                                       .fileSize(fileSize).extension(extension).fileHash(fileHash).zipMaster(zipMaster)
                                       .sourceType(SourceType.UPLOADED)
                                       .fileProcessingStatus(FileProcessingStatus.DUPLICATE)
                                       .duplicateOfFileId(duplicateOfId).fileLocation("N/A").build();
        fileMasterRepository.save(skipped);
    }
}