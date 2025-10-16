package com.eyelevel.documentprocessor.service;

import com.eyelevel.documentprocessor.common.apiclient.gx.GXApiClient;
import com.eyelevel.documentprocessor.dto.gx.creategxbucket.response.GXBucket;
import com.eyelevel.documentprocessor.exception.DocumentProcessingException;
import com.eyelevel.documentprocessor.exception.MessageProcessingFailedException;
import com.eyelevel.documentprocessor.model.*;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.ZipMasterRepository;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

@Slf4j
@Service
@RequiredArgsConstructor
public class ZipExtractionService {

    private final ZipMasterRepository zipMasterRepository;
    private final FileMasterRepository fileMasterRepository;
    private final S3StorageService s3StorageService;
    private final SqsTemplate sqsTemplate;
    private final ValidationService validationService;
    private final GXApiClient gxApiClient;

    @Value("${aws.sqs.file-queue-name}")
    private String fileQueueName;

    private static final int COPY_BUFFER = 8192;
    private static final Set<String> IGNORED_FILES = Set.of("__MACOSX", ".DS_Store", "Thumbs.db");

    @Transactional
    public void extractAndQueueFiles(final Long zipMasterId) {
        ZipMaster zipMaster = zipMasterRepository.findById(zipMasterId)
                .orElseThrow(() -> new IllegalStateException("ZipMaster not found with ID: " + zipMasterId));

        if (zipMaster.getZipProcessingStatus() != ZipProcessingStatus.QUEUED_FOR_EXTRACTION) {
            log.warn("ZipMaster ID {} is not in a processable state ({}).", zipMasterId, zipMaster.getZipProcessingStatus());
            return;
        }

        zipMaster.setZipProcessingStatus(ZipProcessingStatus.EXTRACTION_IN_PROGRESS);
        zipMasterRepository.save(zipMaster);
        log.info("Started extraction for ZipMaster ID: {}", zipMasterId);

        try {
            processZipStream(zipMaster);
            zipMaster.setZipProcessingStatus(ZipProcessingStatus.EXTRACTION_SUCCESS);
            log.info("Successfully completed ZIP extraction for ZipMaster ID: {}", zipMasterId);
        } catch (ZipException e) {
            log.error("ZIP format error for ZipMaster ID {}: {}", zipMasterId, e.getMessage(), e);
            zipMaster.setZipProcessingStatus(ZipProcessingStatus.EXTRACTION_FAILED);
            zipMaster.setErrorMessage("Invalid ZIP archive: " + e.getMessage());
        } catch (DocumentProcessingException e) {
            log.error("Processing error for ZipMaster ID {}: {}", zipMasterId, e.getMessage(), e);
            zipMaster.setZipProcessingStatus(ZipProcessingStatus.EXTRACTION_FAILED);
            zipMaster.setErrorMessage(e.getMessage());
        } catch (Exception e) {
            log.error("Transient error while processing ZipMaster ID {}. Triggering retry.", zipMasterId, e);
            throw new MessageProcessingFailedException("Failed to process ZIP for ZipMaster ID " + zipMasterId, e);
        } finally {
            zipMasterRepository.save(zipMaster);
        }
    }

    private void processZipStream(final ZipMaster zipMaster) throws IOException {
        try (InputStream s3InputStream = s3StorageService.downloadStream(zipMaster.getOriginalFilePath());
             ZipInputStream zipInputStream = new ZipInputStream(s3InputStream)) {

            if (zipMaster.getProcessingJob().isBulkUpload()) {
                handleBulkUpload(zipInputStream, zipMaster);
            } else {
                handleSingleUpload(zipInputStream, zipMaster);
            }
        }
    }

    private void handleSingleUpload(final ZipInputStream zipInputStream, final ZipMaster zipMaster) throws IOException {
        BiConsumer<ZipEntry, Path> processor = (entry, tempPath) ->
                createAndQueueFileMaster(new ExtractedFileItemFromPath(entry.getName(), tempPath),
                        zipMaster.getProcessingJob(), zipMaster.getGxBucketId(), zipMaster);
        processZipEntries(zipInputStream, processor, false);
    }

    private void handleBulkUpload(final ZipInputStream zipInputStream, final ZipMaster zipMaster) throws IOException {
        Map<String, GXBucket> bucketCache = new HashMap<>();

        BiConsumer<ZipEntry, Path> processor = (entry, tempPath) -> {
            String normalizedPath = entry.getName();
            int separator = normalizedPath.indexOf('/');
            if (separator == -1) {
                log.warn("[BULK] Skipping root-level file '{}'", normalizedPath);
                return;
            }

            String bucketName = normalizedPath.substring(0, separator);
            if (bucketName.isBlank() || bucketName.startsWith(".")) {
                log.warn("[BULK] Skipping hidden/blank bucket for file '{}'", normalizedPath);
                return;
            }

            try {
                GXBucket gxBucket = bucketCache.computeIfAbsent(bucketName, b -> {
                    log.info("[BULK] Creating/retrieving bucket '{}'", b);
                    return gxApiClient.createGXBucket(b);
                });

                createAndQueueFileMaster(new ExtractedFileItemFromPath(normalizedPath, tempPath),
                        zipMaster.getProcessingJob(), gxBucket.bucket().bucketId(), zipMaster);

            } catch (Exception e) {
                log.error("[BULK] Failed processing '{}': {}", normalizedPath, e.getMessage(), e);
                try {
                    Files.deleteIfExists(tempPath);
                } catch (IOException ignored) {
                }
            }
        };

        processZipEntries(zipInputStream, processor, true);
    }

    private void processZipEntries(ZipInputStream zis,
                                   BiConsumer<ZipEntry, Path> fileProcessor,
                                   boolean isBulk) throws IOException {

        ZipEntry entry;
        AtomicBoolean processedAny = new AtomicBoolean(false);

        while ((entry = zis.getNextEntry()) != null) {
            String normalizedPath = entry.getName().replace('\\', '/');

            if (shouldSkipEntry(entry, normalizedPath)) {
                zis.closeEntry();
                continue;
            }

            if (isBulk && !processedAny.get() && !normalizedPath.contains("/")) {
                throw new DocumentProcessingException("Invalid bulk ZIP: file at root: " + normalizedPath);
            }

            Path temp = streamEntryToTempFile(zis, normalizedPath);
            if (Files.size(temp) == 0) {
                Files.deleteIfExists(temp);
                zis.closeEntry();
                continue;
            }

            processedAny.set(true);
            try {
                processSingleEntryStreaming(normalizedPath, temp, fileProcessor, isBulk);
            } finally {
                zis.closeEntry();
            }
        }

        if (isBulk && !processedAny.get()) {
            throw new DocumentProcessingException("Bulk ZIP is empty or contains only ignored files/directories");
        }
    }

    private boolean shouldSkipEntry(ZipEntry entry, String normalizedPath) {
        if (entry.isDirectory() || normalizedPath.endsWith("/")) return true;

        String[] parts = normalizedPath.split("/");
        String root = parts.length > 0 ? parts[0] : normalizedPath;
        String fileName = parts.length > 0 ? parts[parts.length - 1] : normalizedPath;

        if (IGNORED_FILES.contains(root) || IGNORED_FILES.contains(fileName)) {
            log.debug("Ignoring metadata entry: {}", normalizedPath);
            return true;
        }
        return false;
    }

    private Path streamEntryToTempFile(ZipInputStream zis, String entryName) throws IOException {
        String suffix = entryName.contains(".") ? entryName.substring(entryName.lastIndexOf('.')) : ".bin";
        Path tempFile = Files.createTempFile("zip-entry-", suffix);

        try (var out = Files.newOutputStream(tempFile);
             var bos = new java.io.BufferedOutputStream(out, COPY_BUFFER)) {

            byte[] buf = new byte[COPY_BUFFER];
            int read;
            while ((read = zis.read(buf)) != -1) bos.write(buf, 0, read);
            bos.flush();
        } catch (IOException e) {
            Files.deleteIfExists(tempFile);
            throw e;
        }

        return tempFile;
    }

    private void processSingleEntryStreaming(String normalizedPath,
                                             Path tempFile,
                                             BiConsumer<ZipEntry, Path> fileProcessor,
                                             boolean isBulk) throws IOException {

        ZipEntry entry = new ZipEntry(normalizedPath);
        if (normalizedPath.toLowerCase().endsWith(".zip")) {
            log.debug("Processing nested ZIP: {} (temp: {})", normalizedPath, tempFile);
            try (var nestedFis = Files.newInputStream(tempFile);
                 var nestedZis = new ZipInputStream(nestedFis)) {
                processZipEntries(nestedZis, fileProcessor, isBulk);
            } finally {
                Files.deleteIfExists(tempFile);
            }
        } else {
            fileProcessor.accept(entry, tempFile);
        }
    }

    private void createAndQueueFileMaster(ExtractedFileItemFromPath item,
                                          ProcessingJob job,
                                          Integer gxBucketId,
                                          ZipMaster zipMaster) {

        Path tempFile = item.getPath();
        String normalizedPath = item.getFilename();
        String fileName = FilenameUtils.getName(normalizedPath);

        if (fileName.startsWith(".")) {
            try {
                Files.deleteIfExists(tempFile);
            } catch (IOException ignored) {
                
            }
            return;
        }

        try {
            long fileSize = Files.size(tempFile);
            String validationError = validationService.validateFile(fileName, fileSize);
            if (validationError != null) {
                FileMaster ignored = FileMaster.builder()
                        .processingJob(job).gxBucketId(gxBucketId)
                        .fileName(fileName).fileSize(fileSize)
                        .extension(FilenameUtils.getExtension(fileName).toLowerCase())
                        .fileProcessingStatus(FileProcessingStatus.IGNORED)
                        .sourceType(SourceType.UPLOADED)
                        .zipMaster(zipMaster)
                        .errorMessage(validationError)
                        .fileLocation("N/A - IGNORED")
                        .build();
                fileMasterRepository.save(ignored);
                Files.deleteIfExists(tempFile);
                return;
            }

            String fileHash = computeSha256Hex(tempFile);
            Optional<FileMaster> duplicate = fileMasterRepository.findFirstByGxBucketIdAndFileHashAndFileProcessingStatus(
                    gxBucketId, fileHash, FileProcessingStatus.COMPLETED
            );

            if (duplicate.isPresent()) {
                FileMaster skipped = FileMaster.builder()
                        .processingJob(job).gxBucketId(gxBucketId)
                        .fileName(fileName).fileSize(fileSize)
                        .extension(FilenameUtils.getExtension(fileName).toLowerCase())
                        .fileHash(fileHash)
                        .zipMaster(zipMaster)
                        .sourceType(SourceType.UPLOADED)
                        .fileProcessingStatus(FileProcessingStatus.SKIPPED_DUPLICATE)
                        .duplicateOfFileId(duplicate.get().getId())
                        .fileLocation("N/A - DUPLICATE")
                        .build();
                fileMasterRepository.save(skipped);
                Files.deleteIfExists(tempFile);
                return;
            }

            String s3Key = S3StorageService.constructS3Key(fileName, gxBucketId, job.getId(), "files");
            try (var fis = Files.newInputStream(tempFile)) {
                s3StorageService.upload(s3Key, fis, fileSize);
            }

            FileMaster newFile = FileMaster.builder()
                    .processingJob(job).gxBucketId(gxBucketId)
                    .fileName(fileName).fileSize(fileSize)
                    .extension(FilenameUtils.getExtension(fileName).toLowerCase())
                    .fileHash(fileHash).originalContentHash(fileHash)
                    .fileLocation(s3Key)
                    .sourceType(SourceType.UPLOADED)
                    .zipMaster(zipMaster)
                    .fileProcessingStatus(FileProcessingStatus.QUEUED)
                    .build();
            fileMasterRepository.save(newFile);

            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    sqsTemplate.send(fileQueueName, Map.of("fileMasterId", newFile.getId()));
                    try {
                        Files.deleteIfExists(tempFile);
                    } catch (IOException ignored) {
                    }
                }
            });

        } catch (IOException e) {
            try {
                Files.deleteIfExists(tempFile);
            } catch (IOException ignored) {
            }
            throw new DocumentProcessingException("Failed to process file: " + normalizedPath, e);
        }
    }

    private String computeSha256Hex(Path file) throws IOException {
        try (var fis = Files.newInputStream(file)) {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (DigestInputStream dis = new DigestInputStream(fis, md)) {
                byte[] buffer = new byte[COPY_BUFFER];
                while (dis.read(buffer) != -1) {
                }
            }
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder(2 * digest.length);
            for (byte b : digest) sb.append(String.format("%02x", b & 0xff));
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 algorithm not found", e);
        }
    }

    @Getter
    private static class ExtractedFileItemFromPath {
        private final String filename;
        private final Path path;

        public ExtractedFileItemFromPath(String filename, Path path) {
            this.filename = filename;
            this.path = path;
        }

    }
}
