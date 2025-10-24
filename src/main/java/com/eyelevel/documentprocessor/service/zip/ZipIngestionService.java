package com.eyelevel.documentprocessor.service.zip;

import com.eyelevel.documentprocessor.common.apiclient.gx.GXApiClient;
import com.eyelevel.documentprocessor.dto.gx.creategxbucket.response.GXBucket;
import com.eyelevel.documentprocessor.exception.DocumentProcessingException;
import com.eyelevel.documentprocessor.exception.MessageProcessingFailedException;
import com.eyelevel.documentprocessor.model.*;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.ZipMasterRepository;
import com.eyelevel.documentprocessor.service.asynctask.AsyncTaskManager;
import com.eyelevel.documentprocessor.service.asynctask.FileMasterPostUploadAction;
import com.eyelevel.documentprocessor.service.file.FileMasterAtomicService;
import com.eyelevel.documentprocessor.service.file.ValidationService;
import com.eyelevel.documentprocessor.service.s3.S3StorageService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.dao.DataIntegrityViolationException;
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

@Slf4j
@Service
public class ZipIngestionService {

    @Value("${app.processing.zip-handler.concurrency-limit}")
    private int concurrencyLimit;
    @Value("${app.processing.zip-handler.temp-dir}")
    private String tempFileDir;

    private final ZipMasterRepository zipMasterRepository;
    private final FileMasterRepository fileMasterRepository;
    private final S3StorageService s3StorageService;
    private final ValidationService validationService;
    private final GXApiClient gxApiClient;
    private final ZipStreamProcessor zipStreamProcessor;
    private final FileMasterAtomicService fileMasterAtomicService;
    private final AsyncTaskManager asyncTaskManager;
    private final FileMasterPostUploadAction fileMasterPostUploadAction;
    private ZipIngestionService self;

    public ZipIngestionService(ZipMasterRepository zipMasterRepository,
                               FileMasterRepository fileMasterRepository,
                               S3StorageService s3StorageService,
                               ValidationService validationService,
                               GXApiClient gxApiClient,
                               ZipStreamProcessor zipStreamProcessor,
                               FileMasterAtomicService fileMasterAtomicService,
                               AsyncTaskManager asyncTaskManager,
                               FileMasterPostUploadAction fileMasterPostUploadAction) {
        this.zipMasterRepository = zipMasterRepository;
        this.fileMasterRepository = fileMasterRepository;
        this.s3StorageService = s3StorageService;
        this.validationService = validationService;
        this.gxApiClient = gxApiClient;
        this.zipStreamProcessor = zipStreamProcessor;
        this.fileMasterAtomicService = fileMasterAtomicService;
        this.asyncTaskManager = asyncTaskManager; // <-- NEW DEPENDENCY
        this.fileMasterPostUploadAction = fileMasterPostUploadAction;
    }

    @Autowired
    public void setSelf(@Lazy ZipIngestionService self) {
        this.self = self;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processZipEntry(ZipStreamProcessor.ZipEntryWorkItem workItem, Integer gxBucketId, Long zipMasterId) {
        final ZipMaster zipMaster = zipMasterRepository.findById(zipMasterId)
                .orElseThrow(() -> new IllegalStateException("Cannot process entry: ZipMaster not found with ID " + zipMasterId));
        final ProcessingJob job = zipMaster.getProcessingJob();

        final String fileName = FilenameUtils.getName(workItem.normalizedPath());
        final String extension = FilenameUtils.getExtension(fileName).toLowerCase();
        final long fileSize = workItem.fileSize();
        final String fileHash = workItem.sha256Hash();

        try {
            final String validationError = validationService.validateFileFully(fileName, fileSize);
            if (validationError != null) {
                saveIgnoredFile(job, gxBucketId, zipMaster, fileName, fileSize, extension, validationError);
                cleanupTempFile(workItem.tempFilePath());
                return;
            }

            final Optional<FileMaster> existingWinner = fileMasterAtomicService.findWinner(gxBucketId, fileHash);
            if (existingWinner.isPresent()) {
                handleDuplicate(existingWinner.get(), job, gxBucketId, zipMaster, fileName, fileSize, extension, fileHash);
                cleanupTempFile(workItem.tempFilePath());
                return;
            }

            final String s3Key = S3StorageService.constructS3Key(fileName, gxBucketId, job.getId(), "files");
            FileMaster potentialNewFile = buildFileMaster(job, gxBucketId, zipMaster, fileName, fileSize, extension, fileHash, s3Key);

            try {
                FileMaster newFile = fileMasterAtomicService.attemptToCreate(potentialNewFile);

                // SIMPLIFIED: Delegate to the AsyncTaskManager.
                asyncTaskManager.scheduleUploadAfterCommit(
                        newFile.getId(),
                        s3Key,
                        workItem.tempFilePath(),
                        fileMasterPostUploadAction
                );

            } catch (DataIntegrityViolationException ex) {
                log.warn("Race condition for hash '{}'. Recovering by finding winner.", fileHash);
                FileMaster winner = fileMasterAtomicService.findWinner(gxBucketId, fileHash)
                        .orElseThrow(() -> new IllegalStateException("FATAL: Race condition occurred, but winner not found for hash " + fileHash, ex));
                handleDuplicate(winner, job, gxBucketId, zipMaster, fileName, fileSize, extension, fileHash);
                cleanupTempFile(workItem.tempFilePath());
            }
        } catch (Exception e) {
            cleanupTempFile(workItem.tempFilePath());
            throw new DocumentProcessingException("Failed to process entry: " + workItem.normalizedPath(), e);
        }
    }

    // --- All other methods in this file are unchanged ---

    public void ingestAndQueueFiles(final Long zipMasterId) {
        Optional<ZipMaster> zipMasterOpt = self.findAndPrepareZipMasterForIngestion(zipMasterId);
        if (zipMasterOpt.isEmpty()) {
            return;
        }
        final ZipMaster zipMaster = zipMasterOpt.get();
        log.info("Beginning ingestion for ZipMaster ID: {}. Concurrency limit: {}", zipMasterId, concurrencyLimit);

        prepareProcessingEnvironment();

        try {
            processZipStreamConcurrently(zipMaster);
            self.updateZipStatus(zipMasterId, ZipProcessingStatus.EXTRACTION_SUCCESS, null);
            log.info("Successfully completed ingestion for ZipMaster ID: {}", zipMasterId);
        } catch (Exception e) {
            handleStreamProcessingFailure(zipMasterId, e);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Optional<ZipMaster> findAndPrepareZipMasterForIngestion(Long zipMasterId) {
        final ZipMaster zipMaster = zipMasterRepository.findByIdWithJob(zipMasterId)
                .orElseThrow(() -> new IllegalStateException("ZipMaster not found with ID: " + zipMasterId));

        if (zipMaster.getProcessingJob().getStatus() == ProcessingStatus.TERMINATED) {
            log.warn("Skipping ingestion for ZipMaster ID {}: Job was terminated.", zipMasterId);
            if (zipMaster.getZipProcessingStatus() != ZipProcessingStatus.TERMINATED) {
                zipMaster.setZipProcessingStatus(ZipProcessingStatus.TERMINATED);
                zipMaster.setErrorMessage("Job terminated by user.");
                zipMasterRepository.save(zipMaster);
            }
            return Optional.empty();
        }

        if (zipMaster.getZipProcessingStatus() != ZipProcessingStatus.QUEUED_FOR_EXTRACTION) {
            log.warn("ZipMaster ID {} is not in a processable state ({}), skipping.", zipMasterId, zipMaster.getZipProcessingStatus());
            return Optional.empty();
        }

        zipMaster.setZipProcessingStatus(ZipProcessingStatus.EXTRACTION_IN_PROGRESS);
        zipMasterRepository.save(zipMaster);
        return Optional.of(zipMaster);
    }

    private void processZipStreamConcurrently(ZipMaster zipMaster) throws IOException {
        final Semaphore semaphore = new Semaphore(concurrencyLimit);
        final Map<String, GXBucket> bucketCache = new ConcurrentHashMap<>();

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
             InputStream s3InputStream = s3StorageService.downloadStream(zipMaster.getOriginalFilePath())) {

            final Consumer<ZipStreamProcessor.ZipEntryWorkItem> workItemConsumer =
                    (item) -> submitEntryForProcessing(item, zipMaster, bucketCache, executor, semaphore);

            zipStreamProcessor.processStream(s3InputStream, Paths.get(tempFileDir), workItemConsumer);
        }
    }

    private void submitEntryForProcessing(ZipStreamProcessor.ZipEntryWorkItem item, ZipMaster zipMaster, Map<String, GXBucket> bucketCache, ExecutorService executor, Semaphore semaphore) {
        try {
            semaphore.acquire();
            log.debug("Acquired permit for entry: {}. Remaining permits: {}", item.normalizedPath(), semaphore.availablePermits());

            executor.submit(() -> {
                try {
                    processSingleEntry(item, zipMaster, bucketCache);
                } catch (Exception e) {
                    log.error("Unhandled exception processing entry '{}' in ZipMaster ID {}.", item.normalizedPath(), zipMaster.getId(), e);
                    cleanupTempFile(item.tempFilePath());
                } finally {
                    semaphore.release();
                    log.debug("Released permit for entry: {}. Remaining permits: {}", item.normalizedPath(), semaphore.availablePermits());
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while waiting for semaphore permit for entry '{}'.", item.normalizedPath(), e);
            cleanupTempFile(item.tempFilePath());
            throw new RuntimeException(e);
        }
    }

    private void processSingleEntry(ZipStreamProcessor.ZipEntryWorkItem item, ZipMaster zipMaster, Map<String, GXBucket> bucketCache) {
        final ProcessingJob job = zipMaster.getProcessingJob();
        final Integer gxBucketId = job.isBulkUpload()
                ? getBucketIdForBulkUpload(item.normalizedPath(), bucketCache)
                : zipMaster.getGxBucketId();

        if (gxBucketId != null) {
            self.processZipEntry(item, gxBucketId, zipMaster.getId());
        } else {
            cleanupTempFile(item.tempFilePath());
        }
    }

    private void prepareProcessingEnvironment() {
        try {
            Files.createDirectories(Paths.get(tempFileDir));
        } catch (IOException e) {
            throw new DocumentProcessingException("Failed to create temporary directory: " + tempFileDir, e);
        }
    }

    private void handleStreamProcessingFailure(Long zipMasterId, Exception e) {
        String errorMsg = "A processing error occurred during ZIP extraction: " + e.getMessage();
        self.updateZipStatus(zipMasterId, ZipProcessingStatus.EXTRACTION_FAILED, errorMsg);
        log.error("Ingestion failed for ZipMaster ID {}: A processing error occurred.", zipMasterId, e);

        if (!(e instanceof ZipException)) {
            throw new MessageProcessingFailedException("Failed to process ZIP for ZipMaster ID " + zipMasterId, e);
        }
    }

    private Integer getBucketIdForBulkUpload(String entryName, Map<String, GXBucket> bucketCache) {
        final int separator = entryName.indexOf('/');
        if (separator == -1) {
            log.warn("[BULK] Skipping root-level file '{}' as it's not in a bucket folder.", entryName);
            return null;
        }
        final String bucketName = entryName.substring(0, separator);
        if (bucketName.isBlank() || bucketName.startsWith(".")) {
            log.warn("[BULK] Skipping hidden/blank bucket for file '{}'", entryName);
            return null;
        }
        return bucketCache.computeIfAbsent(bucketName, gxApiClient::createGXBucket).bucket().bucketId();
    }

    private FileMaster buildFileMaster(ProcessingJob job, Integer gxBucketId, ZipMaster zipMaster, String fileName, long fileSize, String extension, String fileHash, String s3Key) {
        return FileMaster.builder()
                .processingJob(job)
                .gxBucketId(gxBucketId)
                .fileName(fileName)
                .fileSize(fileSize)
                .extension(extension)
                .fileHash(fileHash)
                .sourceType(SourceType.UPLOADED)
                .zipMaster(zipMaster)
                .fileProcessingStatus(FileProcessingStatus.QUEUED)
                .fileLocation(s3Key)
                .build();
    }

    private void handleDuplicate(FileMaster existingDuplicate, ProcessingJob job, Integer gxBucketId, ZipMaster zipMaster, String fileName, long fileSize, String extension, String fileHash) {
        log.warn("DUPLICATE DETECTED for file '{}'. It is a duplicate of FileMaster ID {}. Creating new duplicate record.", fileName, existingDuplicate.getId());
        saveSkippedDuplicateFile(job, gxBucketId, zipMaster, fileName, fileSize, extension, fileHash, existingDuplicate.getId());
    }

    private void saveIgnoredFile(ProcessingJob job, Integer gxBucketId, ZipMaster zipMaster, String fileName, long fileSize, String extension, String errorMessage) {
        FileMaster ignored = FileMaster.builder()
                .processingJob(job).gxBucketId(gxBucketId).fileName(fileName).fileSize(fileSize).extension(extension)
                .fileProcessingStatus(FileProcessingStatus.IGNORED)
                .sourceType(SourceType.UPLOADED).zipMaster(zipMaster).errorMessage(errorMessage)
                .fileLocation("N/A").build();
        fileMasterRepository.save(ignored);
    }

    private void saveSkippedDuplicateFile(ProcessingJob job, Integer gxBucketId, ZipMaster zipMaster, String fileName, long fileSize, String extension, String fileHash, Long duplicateOfId) {
        FileMaster skipped = FileMaster.builder()
                .processingJob(job).gxBucketId(gxBucketId).fileName(fileName).fileSize(fileSize).extension(extension)
                .fileHash(fileHash).zipMaster(zipMaster).sourceType(SourceType.UPLOADED)
                .fileProcessingStatus(FileProcessingStatus.DUPLICATE)
                .duplicateOfFileId(duplicateOfId).fileLocation("N/A").build();
        fileMasterRepository.save(skipped);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateZipStatus(Long zipMasterId, ZipProcessingStatus status, String errorMessage) {
        ZipMaster zipMaster = zipMasterRepository.findById(zipMasterId)
                .orElseThrow(() -> new IllegalStateException("Cannot update status. ZipMaster not found: " + zipMasterId));
        zipMaster.setZipProcessingStatus(status);
        if (errorMessage != null) {
            zipMaster.setErrorMessage(errorMessage);
        }
        zipMasterRepository.save(zipMaster);
    }

    private void cleanupTempFile(Path tempFile) {
        if (tempFile != null) {
            try {
                Files.deleteIfExists(tempFile);
            } catch (IOException e) {
                log.error("CRITICAL: Failed to delete temporary file: {}", tempFile, e);
            }
        }
    }
}