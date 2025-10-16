package com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler;

import com.eyelevel.documentprocessor.config.DocumentProcessingConfig;
import com.eyelevel.documentprocessor.exception.FileConversionException;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Service
@Slf4j
@RequiredArgsConstructor
public class GhostscriptOptimizer {

    private final DocumentProcessingConfig config;

    private record StreamGobbler(InputStream inputStream, Consumer<String> consumer) implements Runnable {
        @Override
        public void run() {
            new BufferedReader(new InputStreamReader(inputStream)).lines().forEach(consumer);
        }
    }

    @Retryable(
            retryFor = {FileConversionException.class},
            maxAttemptsExpression = "#{${app.processing.pdf.ghostscript.retry-attempts} + 1}",
            backoff = @Backoff(delayExpression = "#{${app.processing.pdf.ghostscript.retry-delay-seconds} * 1000}"),
            listeners = {"ghostscriptRetryListener"} // Optional: for logging retries
    )
    public File optimize(File fileToOptimize) throws FileConversionException, IOException {
        long timeout = config.getPdf().getGhostscript().getOptimizationTimeoutMinutes();
        int threads = Math.max(1, Runtime.getRuntime().availableProcessors() / 2); // Use half of the cores
        log.info("Optimizing PDF '{}' using Ghostscript (Timeout: {}m, Threads: {}).", fileToOptimize.getName(), timeout, threads);

        Path parentDir = fileToOptimize.getParentFile().toPath();
        File outputFile = Files.createTempFile(parentDir, "gs-opt-", ".pdf").toFile();

        try {
            List<String> command = new ArrayList<>(List.of(
                    "gs", "-sDEVICE=pdfwrite", "-dCompatibilityLevel=1.4",
                    "-dPDFSETTINGS=" + config.getPdf().getGhostscript().getPreset(),
                    "-dNOPAUSE", "-dBATCH", "-dDetectDuplicateImages=true",
                    "-dNumRenderingThreads=" + threads,
                    "-sOutputFile=" + outputFile.getAbsolutePath(),
                    fileToOptimize.getAbsolutePath()
            ));

            Process process = new ProcessBuilder(command).start();
            handleProcessExecution(process, fileToOptimize.getName(), timeout);

            if (!outputFile.exists() || outputFile.length() == 0) {
                log.warn("Ghostscript optimization resulted in an empty file for '{}'.", fileToOptimize.getName());
                return fileToOptimize; // Return original
            }
            return logAndReplaceFile(fileToOptimize, outputFile);
        } finally {
            Files.deleteIfExists(outputFile.toPath());
        }
    }

    @Recover
    public File recover(FileConversionException e, File fileToOptimize) {
        log.error("Ghostscript optimization failed for '{}' after all retry attempts. Retaining original file.", fileToOptimize.getName(), e);
        // Gracefully recover by returning the original file, allowing the pipeline to continue.
        return fileToOptimize;
    }

    @SneakyThrows
    private void handleProcessExecution(Process process, String fileName, long timeoutMinutes) {
        // This is the same robust process handler you wrote before
        StringBuilder errorOutput = new StringBuilder();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?> stdoutFuture = executor.submit(new StreamGobbler(process.getInputStream(), log::debug));
            Future<?> stderrFuture = executor.submit(
                    new StreamGobbler(process.getErrorStream(), line -> {
                        log.warn("[Ghostscript-stderr] {}", line);
                        errorOutput.append(line).append("\n");
                    }));

            if (!process.waitFor(timeoutMinutes, TimeUnit.MINUTES)) {
                process.destroyForcibly();
                log.error("Ghostscript optimization timed out after {} minutes for file: {}", timeoutMinutes, fileName);
                throw new FileConversionException("Ghostscript optimization timed out for file: " + fileName);
            }

            stdoutFuture.get(5, TimeUnit.SECONDS);
            stderrFuture.get(5, TimeUnit.SECONDS);

            if (process.exitValue() != 0) {
                String detailedError = "Ghostscript optimization failed for file: %s. Exit code: %d. Reason: %s".formatted(
                        fileName, process.exitValue(), errorOutput.toString().trim());
                log.error(detailedError);
                throw new FileConversionException(detailedError);
            }
        }
    }

    @SneakyThrows
    private File logAndReplaceFile(File originalFile, File optimizedFile) {
        // This is the same file replacement logic you wrote
        long originalSize = originalFile.length();
        long newSize = optimizedFile.length();
        if (newSize > 0 && newSize < originalSize) {
            double reduction = (100.0 * (originalSize - newSize) / originalSize);
            log.info("Successfully optimized '{}'. Size reduced by {}% ({} -> {} bytes).",
                    originalFile.getName(), String.format("%.2f", reduction), originalSize, newSize);
            Files.move(optimizedFile.toPath(), originalFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return originalFile;
        } else {
            log.warn("Optimization did not reduce file size for '{}'. Original: {}, Optimized: {}. Retaining original.",
                    originalFile.getName(), originalSize, newSize);
            return originalFile;
        }
    }
}