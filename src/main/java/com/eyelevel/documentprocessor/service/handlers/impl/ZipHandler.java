package com.eyelevel.documentprocessor.service.handlers.impl;

import com.eyelevel.documentprocessor.model.ExtractedFileItem;
import com.eyelevel.documentprocessor.model.FileMaster;
import com.eyelevel.documentprocessor.service.handlers.FileHandler;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * A file handler that processes ZIP archives by extracting all contained files.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ZipHandler implements FileHandler {

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supports(String extension) {
        boolean isSupported = "zip".equalsIgnoreCase(extension);
        log.trace("Checking ZIP support for extension '{}': {}", extension, isSupported);
        return isSupported;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SneakyThrows
    public List<ExtractedFileItem> handle(InputStream inputStream, FileMaster context) {
        long jobId = context.getProcessingJob().getId();
        long fileMasterId = context.getId();
        log.info("[JobId: {}, FileMasterId: {}] Starting ZIP file unpacking for '{}'.", jobId, fileMasterId, context.getFileName());

        List<ExtractedFileItem> extractedItems = new ArrayList<>();
        Path tempDir = Files.createTempDirectory("zip-unpack-" + fileMasterId + "-");
        log.debug("[JobId: {}, FileMasterId: {}] Created temporary directory for unpacking: {}", jobId, fileMasterId, tempDir);

        try (ZipInputStream zis = new ZipInputStream(inputStream)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    log.debug("[JobId: {}, FileMasterId: {}] Skipping directory entry in zip: {}", jobId, fileMasterId, entry.getName());
                    continue;
                }

                log.debug("[JobId: {}, FileMasterId: {}] Extracting file entry: {}", jobId, fileMasterId, entry.getName());
                Path tempFile = tempDir.resolve(entry.getName()).normalize();
                Files.createDirectories(tempFile.getParent());
                Files.copy(zis, tempFile, StandardCopyOption.REPLACE_EXISTING);

                byte[] content = Files.readAllBytes(tempFile);
                extractedItems.add(new ExtractedFileItem(entry.getName(), content));

                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException e) {
                    log.warn("[JobId: {}, FileMasterId: {}] Failed to delete temporary entry file: {}", jobId, fileMasterId, tempFile, e);
                }

                zis.closeEntry();
            }
        } finally {
            log.debug("[JobId: {}, FileMasterId: {}] Cleaning up main temporary directory: {}", jobId, fileMasterId, tempDir);
            try {
                FileUtils.deleteDirectory(tempDir.toFile());
            } catch (IOException e) {
                log.error("[JobId: {}, FileMasterId: {}] Failed to clean up main temporary directory: {}", jobId, fileMasterId, tempDir, e);
            }
        }
        log.info("[JobId: {}, FileMasterId: {}] Finished unpacking ZIP file. Extracted {} items.", jobId, fileMasterId, extractedItems.size());
        return extractedItems;
    }
}