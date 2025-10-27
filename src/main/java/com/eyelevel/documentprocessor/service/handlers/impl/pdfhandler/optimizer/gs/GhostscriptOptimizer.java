package com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.optimizer.gs;

import com.eyelevel.documentprocessor.common.processexec.ProcessExecutor;
import com.eyelevel.documentprocessor.common.processexec.ProcessExecutor.ProcessResult;
import com.eyelevel.documentprocessor.config.DocumentProcessingConfig;
import com.eyelevel.documentprocessor.exception.FileConversionException;
import com.eyelevel.documentprocessor.exception.FileProtectedException;
import com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.optimizer.PDFOptimizerStrategy;
import com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.optimizer.PdfOptimizer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@Slf4j
@RequiredArgsConstructor
@Service("ghostscriptOptimizer")
public class GhostscriptOptimizer implements PdfOptimizer {

    private static final Pattern PASSWORD_ERROR_PATTERN = Pattern.compile("This file requires a password for access",
            Pattern.CASE_INSENSITIVE);
    private final DocumentProcessingConfig config;
    private final ProcessExecutor processExecutor;

    @Override
    public PDFOptimizerStrategy getStrategyName() {
        return PDFOptimizerStrategy.GHOSTSCRIPT;
    }

    @Override
    @Retryable(retryFor = {FileConversionException.class},
            maxAttemptsExpression = "#{${app.processing.pdf.ghostscript.retry.attempts} + 1}",
            backoff = @Backoff(delayExpression = "#{${app.processing.pdf.ghostscript.retry.delay-ms}}"),
            listeners = {"ghostscriptRetryListener"})
    public File optimize(File inputFile, String contextInfo)
            throws FileConversionException, FileProtectedException, InterruptedException, IOException {
        long timeout = config.getPdf().getGhostscript().getOptimizationTimeoutMinutes();
        log.info("[{}] Attempting to optimize PDF '{}' using {} (Timeout: {}m).", contextInfo, inputFile.getName(),
                getStrategyName(), timeout);

        Path tempOutputFile = null;
        try {
            tempOutputFile = Files.createTempFile(inputFile.toPath().getParent(), "gs-opt-", ".pdf");
            List<String> command = buildOptimizationCommand(inputFile, tempOutputFile.toFile());
            ProcessResult result = processExecutor.execute(command, contextInfo, timeout, "gs");

            if (result.exitCode() != 0) {
                if (PASSWORD_ERROR_PATTERN.matcher(result.stderr()).find()) {
                    throw new FileProtectedException(
                            "Ghostscript failed: file is password protected. " + inputFile.getName());
                }
                throw new FileConversionException(
                        String.format("Ghostscript optimization failed for '%s'. Error: %s", inputFile.getName(),
                                result.stderr()));
            }

            return handleOptimizationResult(inputFile, tempOutputFile.toFile(), contextInfo);
        } catch (Exception e) {
            if (e instanceof FileConversionException || e instanceof FileProtectedException) {
                throw e;
            }
            throw new FileConversionException("Ghostscript optimization process failed for: " + inputFile.getName(), e);
        } finally {
            if (tempOutputFile != null) {
                try {
                    Files.deleteIfExists(tempOutputFile);
                } catch (IOException e) {
                    log.warn("[{}] Failed to delete temporary optimization file: {}", contextInfo, tempOutputFile);
                }
            }
        }
    }

    @Recover
    public File recover(FileConversionException e, File inputFile, String contextInfo) {
        log.error("[{}] {} failed for '{}' after all retry attempts. Retaining original file.", contextInfo,
                getStrategyName(), inputFile.getName(), e);
        return inputFile;
    }

    @Recover
    public File recover(FileProtectedException e, File inputFile, String contextInfo) throws FileProtectedException {
        log.error("[{}] {} determined '{}' is password protected. This is a terminal failure.", contextInfo,
                getStrategyName(), inputFile.getName());
        throw e;
    }

    private File handleOptimizationResult(File originalFile, File optimizedFile, String contextInfo)
            throws IOException {
        long originalSize = originalFile.length();
        long newSize = optimizedFile.length();
        if (newSize > 0 && newSize < originalSize) {
            double reduction = 100.0 * (originalSize - newSize) / originalSize;
            log.info("[{}] Successfully optimized '{}'. Size reduced by {} ({} -> {} bytes).", contextInfo,
                    originalFile.getName(), String.format("%.2f%%", reduction), originalSize, newSize);
            Files.move(optimizedFile.toPath(), originalFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return originalFile;
        } else {
            log.warn("[{}] Optimization did not reduce file size for '{}' (Original: {}). Retaining original.",
                    contextInfo, originalFile.getName(), originalSize);
            return originalFile;
        }
    }

    private List<String> buildOptimizationCommand(File inputFile, File outputFile) {
        int threads = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
        return new ArrayList<>(List.of("gs", "-sDEVICE=pdfwrite", "-dCompatibilityLevel=1.4",
                "-dPDFSETTINGS=" + config.getPdf().getGhostscript().getPreset(), "-dNOPAUSE",
                "-dBATCH", "-dDetectDuplicateImages=true", "-dNumRenderingThreads=" + threads,
                "-sOutputFile=" + outputFile.getAbsolutePath(), inputFile.getAbsolutePath()));
    }
}