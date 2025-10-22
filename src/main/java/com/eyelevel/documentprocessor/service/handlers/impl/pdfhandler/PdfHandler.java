package com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler;

import com.eyelevel.documentprocessor.config.DocumentProcessingConfig;
import com.eyelevel.documentprocessor.exception.FileConversionException;
import com.eyelevel.documentprocessor.exception.FileProtectedException;
import com.eyelevel.documentprocessor.model.ExtractedFileItem;
import com.eyelevel.documentprocessor.model.FileMaster;
import com.eyelevel.documentprocessor.service.handlers.FileHandler;
import com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.optimizer.PdfOptimizer;
import com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.splitter.PdfSplitter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
public class PdfHandler implements FileHandler {

    private final DocumentProcessingConfig config;
    private final PdfOptimizer optimizer;
    private final PdfSplitter splitter;

    public PdfHandler(DocumentProcessingConfig config, @Qualifier("pdfOptimizer") PdfOptimizer optimizer,
                      @Qualifier("qpdfSplitter") PdfSplitter splitter) {
        this.config = config;
        this.optimizer = optimizer;
        this.splitter = splitter;
    }

    @Override
    public boolean supports(String extension) {
        return "pdf".equalsIgnoreCase(extension);
    }

    @Override
    public List<ExtractedFileItem> handle(InputStream inputStream, FileMaster context) throws FileConversionException {
        final String contextInfo = String.format("JobId: %d, FileMasterId: %d", context.getProcessingJob().getId(),
                                                 context.getId());
        Path tempDir = null;
        try {
            tempDir = Files.createTempDirectory("pdf-handler-" + context.getId() + "-");
            File workingFile = tempDir.resolve(context.getFileName()).toFile();
            Files.copy(inputStream, workingFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            log.info("[{}] Using '{}' for PDF optimization.", contextInfo, optimizer.getStrategyName());
            workingFile = optimizer.optimize(workingFile, contextInfo);

            if (shouldSplit(workingFile, contextInfo)) {
                log.info("[{}] File '{}' requires splitting. Using '{}' strategy.", contextInfo, workingFile.getName(),
                         splitter.getStrategyName());
                List<File> splitFiles = splitter.split(workingFile, config.getMaxPages(), contextInfo);
                return createExtractedItemsFromFiles(splitFiles);
            }

            log.info("[{}] File processed and does not require splitting.", contextInfo);
            byte[] fileBytes = Files.readAllBytes(workingFile.toPath());
            return List.of(new ExtractedFileItem(workingFile.getName(), fileBytes));

        } catch (FileProtectedException e) {
            log.error("[{}] PDF processing failed because the file is protected. This is a terminal failure.",
                      contextInfo, e);
            throw new FileConversionException(e.getMessage(), e);

        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("[{}] A critical processing error occurred during PDF handling.", contextInfo, e);
            throw new FileConversionException("A critical processing error occurred: " + e.getMessage(), e);

        } finally {
            if (tempDir != null) {
                try {
                    FileUtils.deleteDirectory(tempDir.toFile());
                } catch (IOException e) {
                    log.warn("[{}] Failed to clean up temp dir: {}", contextInfo, tempDir);
                }
            }
        }
    }

    private boolean shouldSplit(File pdfFile, String contextInfo)
    throws FileConversionException, FileProtectedException, IOException, InterruptedException {
        if (pdfFile.length() > config.getMaxFileSize()) return true;
        return splitter.getPageCount(pdfFile, contextInfo) > config.getMaxPages();
    }

    private List<ExtractedFileItem> createExtractedItemsFromFiles(List<File> files) throws IOException {
        List<ExtractedFileItem> items = new ArrayList<>();
        for (File file : files) {
            byte[] bytes = Files.readAllBytes(file.toPath());
            items.add(new ExtractedFileItem(file.getName(), bytes));
        }
        return items;
    }
}