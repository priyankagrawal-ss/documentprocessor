package com.eyelevel.documentprocessor.service.handlers.impl.ziphandler;

import com.eyelevel.documentprocessor.exception.FileConversionException;
import com.eyelevel.documentprocessor.exception.ZipEntryProcessingException;
import com.eyelevel.documentprocessor.model.ExtractedFileItem;
import com.eyelevel.documentprocessor.service.zip.ZipStreamProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.zip.ZipException;

/**
 * A specialized service responsible for extracting the contents of a ZIP file from an InputStream
 * into an in-memory list. This service is designed for use within the main document processing pipeline
 * and includes a retry mechanism for transient stream-related errors.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ZipContentExtractor {

    private final ZipStreamProcessor zipStreamProcessor;

    /**
     * Extracts all valid files from a given ZIP stream into a list of {@link ExtractedFileItem} objects.
     * This method is annotated for retries, which are triggered ONLY for transient {@link IOException}
     * errors that occur during the initial stream processing (e.g., network timeouts).
     *
     * @param inputStream The ZIP file's content as an InputStream.
     * @param contextInfo A string providing context for logging (e.g., Job ID, FileMaster ID).
     * @return A list of {@link ExtractedFileItem}, each containing the name and byte content of a file.
     * @throws IOException             If a transient, retryable I/O error occurs.
     * @throws FileConversionException If a non-retryable error occurs (e.g., corrupt ZIP) or if all retries fail.
     */
    @Retryable(
            retryFor = {IOException.class},
            noRetryFor = {FileConversionException.class, ZipEntryProcessingException.class},
            maxAttemptsExpression = "#{${app.processing.zip-handler.retry.attempts} + 1}",
            backoff = @Backoff(delayExpression = "#{${app.processing.zip-handler.retry.delay-ms}}"),
            listeners = {"zipRetryListener"}
    )
    public List<ExtractedFileItem> extract(InputStream inputStream, String contextInfo) throws IOException, FileConversionException {
        log.info("[{}] Attempting to unpack ZIP stream with retry policy.", contextInfo);
        final List<ExtractedFileItem> extractedItems = new ArrayList<>();

        BiConsumer<String, Path> fileConsumer = (entryName, tempPath) -> {
            try {
                byte[] content = Files.readAllBytes(tempPath);
                extractedItems.add(new ExtractedFileItem(entryName, content));
            } catch (IOException e) {
                throw new ZipEntryProcessingException("Failed to read temp file content for entry: " + entryName, e);
            }
        };

        try {
            zipStreamProcessor.process(inputStream, fileConsumer);
        } catch (ZipException e) {
            log.error("[{}] ZIP file is corrupted and cannot be processed. No retries will be attempted.", contextInfo, e);
            throw new FileConversionException("Invalid or corrupted ZIP archive: " + e.getMessage(), e);
        } catch (IOException e) {
            log.warn("[{}] A transient I/O error occurred during ZIP extraction. A retry will be attempted.", contextInfo, e);
            throw e;
        } catch (ZipEntryProcessingException e) {
            log.error("[{}] Failed to process an individual file entry within the ZIP. This is a non-retryable error.", contextInfo, e);
            throw new FileConversionException(e.getMessage(), e.getCause());
        }

        return extractedItems;
    }

    /**
     * A Spring Retry recovery method, invoked only after all retry attempts for an {@link IOException} have failed.
     */
    @Recover
    public List<ExtractedFileItem> recoverFromIoException(IOException e, InputStream inputStream, String contextInfo) throws FileConversionException {
        String errorMessage = "Failed to extract ZIP after multiple retries due to a persistent I/O error.";
        log.error("[{}] {}", contextInfo, errorMessage, e);
        throw new FileConversionException(errorMessage, e);
    }
}