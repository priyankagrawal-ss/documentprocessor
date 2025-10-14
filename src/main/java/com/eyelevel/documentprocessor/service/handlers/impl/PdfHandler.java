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
import org.apache.commons.io.FilenameUtils;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A file handler that processes PDF files. It can split large PDFs into smaller chunks
 * and optimize them for size using Ghostscript.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class PdfHandler implements FileHandler {

    private final DocumentProcessingConfig config;

    /**
     * A helper class to consume an InputStream in a separate thread, preventing the
     * external process from blocking due to a full I/O buffer.
     */
    private static class StreamGobbler implements Runnable {
        private final InputStream inputStream;
        private final Consumer<String> consumer;

        public StreamGobbler(InputStream inputStream, Consumer<String> consumer) {
            this.inputStream = inputStream;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            new BufferedReader(new InputStreamReader(inputStream)).lines().forEach(consumer);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supports(String extension) {
        boolean isSupported = "pdf".equalsIgnoreCase(extension);
        log.trace("Checking PDF support for extension '{}': {}", extension, isSupported);
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
        log.info("[JobId: {}, FileMasterId: {}] Starting PDF handling process for '{}'.", jobId, fileMasterId, context.getFileName());

        Path tempDir = Files.createTempDirectory("pdf-handler-" + fileMasterId + "-");
        log.debug("[JobId: {}, FileMasterId: {}] Created temporary directory: {}", jobId, fileMasterId, tempDir);
        try {
            File inputFile = new File(tempDir.toFile(), context.getFileName());
            Files.copy(inputStream, inputFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            log.debug("[JobId: {}, FileMasterId: {}] Copied input stream to temporary file '{}'.", jobId, fileMasterId, inputFile.getAbsolutePath());

            try (PDDocument document = Loader.loadPDF(inputFile)) {
                if (shouldSplit(inputFile, document)) {
                    log.info("[JobId: {}, FileMasterId: {}] PDF '{}' requires splitting.", jobId, fileMasterId, context.getFileName());
                    return splitAndOptimizePdf(document, inputFile);
                } else if (config.getPdf().isOptimize()) {
                    log.info("[JobId: {}, FileMasterId: {}] PDF '{}' does not require splitting. Proceeding with optimization.", jobId, fileMasterId, context.getFileName());
                    File optimizedFile = optimizePdf(inputFile);
                    byte[] optimizedBytes = Files.readAllBytes(optimizedFile.toPath());
                    ExtractedFileItem result = new ExtractedFileItem(optimizedFile.getName(), optimizedBytes);
                    return Collections.singletonList(result);
                } else {
                    log.info("[JobId: {}, FileMasterId: {}] PDF splitting and optimization are disabled. No action taken.", jobId, fileMasterId);
                    return Collections.emptyList();
                }
            }
        } finally {
            log.debug("[JobId: {}, FileMasterId: {}] Cleaning up temporary directory: {}", jobId, fileMasterId, tempDir);
            try {
                FileUtils.deleteDirectory(tempDir.toFile());
            } catch (IOException e) {
                log.error("[JobId: {}, FileMasterId: {}] Failed to clean up temporary directory: {}", jobId, fileMasterId, tempDir, e);
            }
        }
    }

    /**
     * Splits a large PDF into multiple smaller documents based on configured size and page limits.
     * Each resulting chunk is then optimized.
     *
     * @param document        The PDDocument object of the original PDF.
     * @param originalPdfFile The original PDF file on disk.
     * @return A list of {@link ExtractedFileItem} containing the content of the new, smaller PDFs.
     */
    @SneakyThrows
    private List<ExtractedFileItem> splitAndOptimizePdf(PDDocument document, File originalPdfFile) {
        int totalPages = document.getNumberOfPages();
        int pagesPerChunk = config.getMaxPages();
        log.info("Splitting PDF with {} pages into chunks of max {} pages.", totalPages, pagesPerChunk);

        List<ExtractedFileItem> splitFiles = new ArrayList<>();
        String baseName = FilenameUtils.getBaseName(originalPdfFile.getName());
        int part = 1;
        for (int startPage = 0; startPage < totalPages; startPage += pagesPerChunk) {
            int endPage = Math.min(startPage + pagesPerChunk, totalPages);
            log.debug("Creating chunk {} (pages {}-{}) for '{}'.", part, startPage + 1, endPage, baseName);
            try (PDDocument chunkDoc = new PDDocument()) {
                for (int i = startPage; i < endPage; i++) {
                    chunkDoc.addPage(document.getPage(i));
                }
                String chunkName = baseName + "_part" + (part++) + ".pdf";
                File chunkFile = new File(originalPdfFile.getParentFile(), chunkName);
                chunkDoc.save(chunkFile);

                File finalChunkFile = chunkFile;
                if (config.getPdf().isOptimize()) {
                    finalChunkFile = optimizePdf(chunkFile);
                }
                byte[] chunkBytes = Files.readAllBytes(finalChunkFile.toPath());
                splitFiles.add(new ExtractedFileItem(chunkName, chunkBytes));
            }
        }
        log.info("Successfully split PDF '{}' into {} parts.", baseName, splitFiles.size());
        return splitFiles;
    }

    /**
     * Determines whether a PDF should be split based on its file size or page count.
     *
     * @param pdfFile  The PDF file on disk.
     * @param document The loaded PDDocument object.
     * @return {@code true} if the file exceeds configured limits, {@code false} otherwise.
     */
    private boolean shouldSplit(File pdfFile, PDDocument document) {
        boolean sizeExceeded = pdfFile.length() > config.getMaxFileSize();
        boolean pagesExceeded = document.getNumberOfPages() > config.getMaxPages();
        if (sizeExceeded) {
            log.warn("PDF size ({} bytes) exceeds max configured size ({} bytes).", pdfFile.length(), config.getMaxFileSize());
        }
        if (pagesExceeded) {
            log.warn("PDF page count ({}) exceeds max configured count ({}).", document.getNumberOfPages(), config.getMaxPages());
        }
        return sizeExceeded || pagesExceeded;
    }

    /**
     * Optimizes a PDF file using the configured tool, which is now exclusively Ghostscript.
     *
     * @param fileToOptimize The file to be optimized.
     * @return The optimized file. This may be the same as the input file if optimization did not reduce size.
     * @throws FileConversionException if the optimization process fails.
     */
    private File optimizePdf(File fileToOptimize) throws FileConversionException {
        log.info("Optimizing PDF '{}' using Ghostscript.", fileToOptimize.getName());
        return optimizeWithGhostscript(fileToOptimize);
    }

    /**
     * Executes the Ghostscript command-line tool to compress and rewrite a PDF file.
     *
     * @param fileToOptimize The file to be optimized.
     * @return The optimized file.
     */
    @SneakyThrows
    private File optimizeWithGhostscript(File fileToOptimize) {
        Path parentDir = fileToOptimize.getParentFile().toPath();
        File outputFile = Files.createTempFile(parentDir, "gs-out-", ".pdf").toFile();
        String preset = config.getPdf().getGhostscript().getPreset();
        try {
            List<String> command = List.of("gs", "-sDEVICE=pdfwrite", "-dCompatibilityLevel=1.4",
                    "-dPDFSETTINGS=" + preset, "-dNOPAUSE", "-dBATCH",
                    "-dDetectDuplicateImages=true",
                    "-sOutputFile=" + outputFile.getAbsolutePath(),
                    fileToOptimize.getAbsolutePath());
            log.debug("Executing Ghostscript command for [{}]: {}", fileToOptimize.getName(), String.join(" ", command));

            Process process = new ProcessBuilder(command).start();
            handleProcessExecution(process, fileToOptimize.getName());

            if (!outputFile.exists() || outputFile.length() == 0) {
                log.warn("Ghostscript optimization resulted in an empty file for '{}'. Retaining original.", fileToOptimize.getName());
                return fileToOptimize;
            }
            return logAndReplaceFile(fileToOptimize, outputFile);
        } finally {
            try {
                Files.deleteIfExists(outputFile.toPath());
            } catch (IOException e) {
                log.error("Failed to delete temporary Ghostscript output file: {}", outputFile.getAbsolutePath(), e);
            }
        }
    }

    /**
     * Manages the execution of an external process, handling its output streams and checking for timeouts and errors.
     *
     * @param process  The external process to manage.
     * @param fileName The name of the file being processed.
     */
    @SneakyThrows
    private void handleProcessExecution(Process process, String fileName) {
        StringBuilder errorOutput = new StringBuilder();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?> stdoutFuture = executor.submit(new StreamGobbler(process.getInputStream(), log::debug));
            Future<?> stderrFuture = executor.submit(
                    new StreamGobbler(process.getErrorStream(), line -> {
                        log.warn("[{}-stderr] {}", "Ghostscript", line);
                        errorOutput.append(line).append("\n");
                    }));

            if (!process.waitFor(2, TimeUnit.MINUTES)) {
                process.destroyForcibly();
                log.error("Ghostscript optimization timed out for file: {}", fileName);
                throw new FileConversionException("Ghostscript optimization timed out for file: " + fileName);
            }

            stderrFuture.get(5, TimeUnit.SECONDS);
            stdoutFuture.get(5, TimeUnit.SECONDS);

            if (process.exitValue() != 0) {
                String detailedError = "Ghostscript optimization failed for file: %s. Exit code: %d. Reason: %s".formatted(
                        fileName, process.exitValue(), errorOutput.toString().trim());
                log.error(detailedError);
                throw new FileConversionException(detailedError);
            }
        }
    }

    /**
     * Compares the original and optimized file sizes. If the optimized file is smaller, it replaces the original.
     *
     * @param originalFile  The original input file.
     * @param optimizedFile The newly created, optimized file.
     * @return The final file (either the new optimized one or the original if it was smaller).
     */
    @SneakyThrows
    private File logAndReplaceFile(File originalFile, File optimizedFile) {
        long originalSize = originalFile.length();
        long newSize = optimizedFile.length();
        try {
            if (newSize < originalSize) {
                double reduction = (100.0 * (originalSize - newSize) / originalSize);
                log.info("Successfully optimized file '{}' using Ghostscript. Size reduced by {}% ({} -> {} bytes).",
                        originalFile.getName(), String.format("%.2f", reduction), originalSize, newSize);
                Files.move(optimizedFile.toPath(), originalFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                return originalFile;
            } else {
                log.warn("Optimization did not reduce file size for '{}'. Original: {} bytes, Optimized: {} bytes. Retaining original.",
                        originalFile.getName(), originalSize, newSize);
                return originalFile;
            }
        } catch (IOException e) {
            log.error(
                    "Failed to replace file '{}' after optimization: {}",
                    originalFile.getName(), e.getMessage(), e
            );
            return originalFile;
        }
    }
}