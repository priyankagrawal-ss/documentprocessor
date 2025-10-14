package com.eyelevel.documentprocessor.service.handlers.impl;

import com.eyelevel.documentprocessor.config.DocumentProcessingConfig;
import com.eyelevel.documentprocessor.exception.FileConversionException;
import com.eyelevel.documentprocessor.model.ExtractedFileItem;
import com.eyelevel.documentprocessor.model.FileMaster;
import com.eyelevel.documentprocessor.service.handlers.FileHandler;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;

/**
 * A file handler that converts various office document formats (e.g., DOCX, PPTX, XLSX) to PDF using LibreOffice.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class LibreOfficeHandler implements FileHandler {

    private final DocumentProcessingConfig config;
    private final LibreOfficeConverter converter;

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supports(String extension) {
        boolean isSupported = config.getLibreoffice().getConvertibleExtensions().contains(extension.toLowerCase());
        log.trace("Checking LibreOffice support for extension '{}': {}", extension, isSupported);
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
        String fileName = context.getFileName();
        log.info("[JobId: {}, FileMasterId: {}] Starting LibreOffice conversion for '{}'.", jobId, fileMasterId, fileName);

        Path taskTempDir = Files.createTempDirectory("lo-task-" + fileMasterId + "-");
        log.debug("[JobId: {}, FileMasterId: {}] Created temporary directory: {}", jobId, fileMasterId, taskTempDir);

        try {
            File inputFile = new File(taskTempDir.toFile(), fileName);
            long bytesWritten = Files.copy(inputStream, inputFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            log.debug("[JobId: {}, FileMasterId: {}] Wrote {} bytes to temporary input file '{}'.", jobId, fileMasterId, bytesWritten, inputFile.getAbsolutePath());

            Path userProfilePath = taskTempDir.resolve("profile");
            Files.createDirectories(userProfilePath);

            File tempPdfFile = converter.convertToPdf(inputFile, taskTempDir.toFile(), userProfilePath);

            if (!tempPdfFile.exists() || tempPdfFile.length() == 0) {
                log.error("[JobId: {}, FileMasterId: {}] LibreOffice conversion resulted in a missing or empty file for '{}'.", jobId, fileMasterId, fileName);
                throw new FileConversionException("Conversion resulted in an empty or missing file for: " + fileName);
            }

            log.info("[JobId: {}, FileMasterId: {}] Successfully converted '{}' to PDF. Reading bytes for next pipeline stage.", jobId, fileMasterId, fileName);
            byte[] pdfBytes = Files.readAllBytes(tempPdfFile.toPath());
            ExtractedFileItem result = new ExtractedFileItem(tempPdfFile.getName(), pdfBytes);
            log.debug("[JobId: {}, FileMasterId: {}] Read {} bytes from converted PDF.", jobId, fileMasterId, pdfBytes.length);

            return Collections.singletonList(result);

        } finally {
            log.debug("[JobId: {}, FileMasterId: {}] Cleaning up temporary directory: {}", jobId, fileMasterId, taskTempDir);
            try {
                FileUtils.deleteDirectory(taskTempDir.toFile());
            } catch (IOException e) {
                log.error("[JobId: {}, FileMasterId: {}] Failed to clean up temporary directory: {}", jobId, fileMasterId, taskTempDir, e);
            }
        }
    }
}