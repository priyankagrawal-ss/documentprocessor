package com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler;

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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class PdfHandler implements FileHandler {

    private final DocumentProcessingConfig config;
    private final GhostscriptOptimizer ghostscriptOptimizer;

    @Override
    public boolean supports(String extension) {
        return "pdf".equalsIgnoreCase(extension);
    }

    @Override
    @SneakyThrows
    public List<ExtractedFileItem> handle(InputStream inputStream, FileMaster context) {
        long jobId = context.getProcessingJob().getId();
        long fileMasterId = context.getId();
        log.info("[JobId: {}, FileMasterId: {}] Starting PDF handling process for '{}'.", jobId, fileMasterId, context.getFileName());

        Path tempDir = Files.createTempDirectory("pdf-handler-" + fileMasterId + "-");
        try {
            File inputFile = new File(tempDir.toFile(), context.getFileName());
            Files.copy(inputStream, inputFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            try (PDDocument document = Loader.loadPDF(inputFile)) {
                if (shouldSplit(inputFile, document)) {
                    log.info("[JobId: {}, FileMasterId: {}] PDF '{}' requires splitting.", jobId, fileMasterId, context.getFileName());
                    return splitAndOptimizePdf(document, inputFile);
                } else if (config.getPdf().isOptimize()) {
                    log.info("[JobId: {}, FileMasterId: {}] PDF '{}' does not require splitting. Proceeding with optimization.", jobId, fileMasterId, context.getFileName());
                    File optimizedFile = optimizePdf(inputFile); // <-- This now calls the retryable service
                    byte[] optimizedBytes = Files.readAllBytes(optimizedFile.toPath());
                    ExtractedFileItem result = new ExtractedFileItem(optimizedFile.getName(), optimizedBytes);
                    return Collections.singletonList(result);
                } else {
                    log.info("[JobId: {}, FileMasterId: {}] PDF splitting and optimization are disabled.", jobId, fileMasterId);
                    return Collections.emptyList();
                }
            }
        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    @SneakyThrows
    private List<ExtractedFileItem> splitAndOptimizePdf(PDDocument document, File originalPdfFile) {
        int totalPages = document.getNumberOfPages();
        int pagesPerChunk = config.getMaxPages();
        List<ExtractedFileItem> splitFiles = new ArrayList<>();
        String baseName = FilenameUtils.getBaseName(originalPdfFile.getName());
        int part = 1;

        for (int startPage = 0; startPage < totalPages; startPage += pagesPerChunk) {
            int endPage = Math.min(startPage + pagesPerChunk, totalPages);
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
        return splitFiles;
    }

    private boolean shouldSplit(File pdfFile, PDDocument document) {
        boolean sizeExceeded = pdfFile.length() > config.getMaxFileSize();
        boolean pagesExceeded = document.getNumberOfPages() > config.getMaxPages();
        return sizeExceeded || pagesExceeded;
    }

    /**
     * Delegates optimization to the GhostscriptOptimizer service.
     * The retry logic is now handled automatically by Spring.
     */
    private File optimizePdf(File fileToOptimize) throws FileConversionException {
        try {
            return ghostscriptOptimizer.optimize(fileToOptimize);
        } catch (IOException e) {
            throw new FileConversionException("Failed to optimize file due to IO error: " + fileToOptimize.getName(), e);
        }
    }
}