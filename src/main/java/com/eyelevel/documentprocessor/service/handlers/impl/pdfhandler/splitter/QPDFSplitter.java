package com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.splitter;

import com.eyelevel.documentprocessor.common.processexec.ProcessExecutor;
import com.eyelevel.documentprocessor.exception.FileConversionException;
import com.eyelevel.documentprocessor.exception.FileProtectedException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@Slf4j
@RequiredArgsConstructor
@Service("qpdfSplitter")
public class QPDFSplitter implements PdfSplitter {

    private static final Pattern QPDF_PASSWORD_ERROR_PATTERN = Pattern.compile("file is encrypted",
            Pattern.CASE_INSENSITIVE);
    private final ProcessExecutor processExecutor;

    @Override
    public PDFSplitterStrategy getStrategyName() {
        return PDFSplitterStrategy.QPDF;
    }

    @Override
    @Retryable(retryFor = {FileConversionException.class, IOException.class, InterruptedException.class},
            maxAttemptsExpression = "#{${app.processing.pdf.qpdf.splitter.retry.attempts} + 1}",
            backoff = @Backoff(delayExpression = "#{${app.processing.pdf.qpdf.splitter.retry.delay-ms}}"),
            listeners = {"qpdfRetryListener"})
    public List<File> split(File inputFile, int pagesPerChunk, String contextInfo)
            throws FileConversionException, FileProtectedException, IOException, InterruptedException {

        int totalPages = getPageCount(inputFile, contextInfo);
        if (totalPages <= pagesPerChunk) {
            return List.of(inputFile);
        }

        log.info("[{}] Splitting PDF '{}' ({} pages) into chunks of {} pages using a compatible qpdf loop.",
                contextInfo, inputFile.getName(), totalPages, pagesPerChunk);

        List<File> outputFiles = new ArrayList<>();
        String baseName = FilenameUtils.getBaseName(inputFile.getName());
        int part = 1;

        try {
            for (int startPage = 1; startPage <= totalPages; startPage += pagesPerChunk) {
                int endPage = Math.min(startPage + pagesPerChunk - 1, totalPages);
                File outputFile = new File(inputFile.getParentFile(), String.format("%s_part%d.pdf", baseName, part++));

                // Build a simple, robust command for a single chunk.
                List<String> command = List.of("qpdf", inputFile.getAbsolutePath(), "--pages", ".",
                        // Represents the input file
                        startPage + "-" + endPage, "--", outputFile.getAbsolutePath());

                // Execute the command for this single chunk.
                ProcessExecutor.ProcessResult result = processExecutor.execute(command, contextInfo, 2, "qpdf");

                if (result.exitCode() != 0) {
                    if (QPDF_PASSWORD_ERROR_PATTERN.matcher(result.stderr()).find()) {
                        throw new FileProtectedException("qpdf failed: file is encrypted. " + inputFile.getName());
                    }
                    throw new FileConversionException(
                            String.format("qpdf splitting failed for '%s' (pages %d-%d). Error: %s",
                                    inputFile.getName(), startPage, endPage, result.stderr()));
                }

                // Verify this one chunk was created.
                if (!outputFile.exists() || outputFile.length() == 0) {
                    throw new FileConversionException(
                            "qpdf did not produce expected output file: " + outputFile.getName());
                }
                outputFiles.add(outputFile);
            }

            log.info("[{}] Successfully split '{}' into {} parts using qpdf.", contextInfo, inputFile.getName(),
                    outputFiles.size());
            return outputFiles;

        } catch (IOException | InterruptedException e) {
            cleanup(outputFiles);
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            throw e;
        } catch (FileConversionException | FileProtectedException e) {
            cleanup(outputFiles);
            throw e;
        } catch (Exception e) {
            cleanup(outputFiles);
            throw new FileConversionException("An unexpected error occurred during qpdf splitting.", e);
        }
    }

    private void cleanup(List<File> files) {
        files.forEach(file -> {
            try {
                Files.deleteIfExists(file.toPath());
            } catch (IOException ex) {
                log.warn("Failed to clean up qpdf split artifact: {}", file.getAbsolutePath());
            }
        });
    }


    @Recover
    public List<File> recover(Exception e, File inputFile, int pagesPerChunk, String contextInfo)
            throws FileConversionException, FileProtectedException {
        if (e instanceof FileProtectedException) {
            log.error("[{}] {} determined '{}' is password protected. This is a terminal failure.", contextInfo,
                    getStrategyName(), inputFile.getName());
            throw (FileProtectedException) e;
        }
        log.error("[{}] {} failed for '{}' after all retry attempts.", contextInfo, getStrategyName(),
                inputFile.getName(), e);
        throw new FileConversionException(
                String.format("%s failed for '%s' after all retries.", getStrategyName(), inputFile.getName()), e);
    }

    @Override
    public int getPageCount(File pdfFile, String contextInfo)
            throws FileConversionException, FileProtectedException, IOException, InterruptedException {
        List<String> command = List.of("qpdf", "--show-npages", pdfFile.getAbsolutePath());
        try {
            ProcessExecutor.ProcessResult result = processExecutor.execute(command, contextInfo, 1, "qpdf");
            if (result.exitCode() == 0 && !result.stdout().isBlank()) {
                return Integer.parseInt(result.stdout());
            }
            if (QPDF_PASSWORD_ERROR_PATTERN.matcher(result.stderr()).find()) {
                throw new FileProtectedException(
                        "qpdf failed to get page count: file is encrypted. " + pdfFile.getName());
            }
            throw new FileConversionException(
                    "qpdf could not determine page count for: " + pdfFile.getName() + ". Error: " + result.stderr());
        } catch (Exception e) {
            if (e instanceof FileConversionException || e instanceof FileProtectedException) throw e;
            throw new FileConversionException("Failed to execute qpdf for page count.", e);
        }
    }
}