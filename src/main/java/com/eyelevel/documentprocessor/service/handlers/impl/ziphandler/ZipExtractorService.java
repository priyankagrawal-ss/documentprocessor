package com.eyelevel.documentprocessor.service.handlers.impl.ziphandler;

import com.eyelevel.documentprocessor.exception.FileConversionException;
import com.eyelevel.documentprocessor.model.ExtractedFileItem;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

@Service
@Slf4j
@RequiredArgsConstructor
public class ZipExtractorService {

    @Retryable(
            retryFor = {IOException.class},
            noRetryFor = {ZipException.class},
            maxAttemptsExpression = "#{${app.processing.zip-handler.retry.attempts} + 1}",
            backoff = @Backoff(delayExpression = "#{${app.processing.zip-handler.retry.delay-ms}}"),
            listeners = {"zipRetryListener"}
    )
    public List<ExtractedFileItem> extract(InputStream inputStream, String contextInfo) throws IOException, FileConversionException {
        log.info("[{}] Attempting to unpack ZIP stream.", contextInfo);
        List<ExtractedFileItem> extractedItems = new ArrayList<>();
        Path tempDir = Files.createTempDirectory("zip-unpack-retry-");

        try (ZipInputStream zis = new ZipInputStream(inputStream)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }

                Path tempFile = tempDir.resolve(entry.getName()).normalize();
                // Ensure parent directories exist for entries like "folder/file.txt"
                Files.createDirectories(tempFile.getParent());
                Files.copy(zis, tempFile, StandardCopyOption.REPLACE_EXISTING);

                byte[] content = Files.readAllBytes(tempFile);
                extractedItems.add(new ExtractedFileItem(entry.getName(), content));

                Files.delete(tempFile); // Clean up immediately to save space
                zis.closeEntry();
            }
        } catch (ZipException e) {
            // This error indicates a corrupt file and should not be retried.
            // We wrap it in our custom exception to fail the process cleanly.
            log.error("[{}] ZIP file is corrupted. No retries will be attempted.", contextInfo, e);
            throw new FileConversionException("Invalid or corrupted ZIP archive: " + e.getMessage(), e);
        } catch (IOException e) {
            log.error("[{}] A transient I/O error occurred during ZIP extraction. A retry will be attempted.", contextInfo, e);
            // Re-throw the raw IOException to trigger the retry mechanism.
            throw e;
        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
        return extractedItems;
    }

    @Recover
    public List<ExtractedFileItem> recoverFromIoException(IOException e, InputStream inputStream, String contextInfo) throws FileConversionException {
        // This is called after all retry attempts for a transient IOException have failed.
        String errorMessage = "Failed to extract ZIP after multiple retries due to a persistent I/O error.";
        log.error("[{}] {}", contextInfo, errorMessage, e);
        throw new FileConversionException(errorMessage, e);
    }

    @Recover
    public List<ExtractedFileItem> recoverFromFileConversionException(FileConversionException e, InputStream inputStream, String contextInfo) throws FileConversionException {
        // This is called immediately (with no retries) if a ZipException was thrown.
        log.error("[{}] ZIP extraction failed and was not retried due to a file format error.", contextInfo);
        throw e; // Re-throw the original, specific exception.
    }
}