package com.eyelevel.documentprocessor.service;

import com.eyelevel.documentprocessor.config.DocumentProcessingConfig;
import com.eyelevel.documentprocessor.exception.FileConversionException;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A service dedicated to converting documents to PDF using an external LibreOffice process.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class LibreOfficeConverter {

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
     * Executes the LibreOffice command-line tool to convert an input file to PDF.
     *
     * @param inputFile       The source file to convert.
     * @param outputDir       The directory where the converted PDF will be saved.
     * @param userProfilePath A path for LibreOffice to use as a temporary user profile, ensuring isolated execution.
     * @return The resulting PDF {@link File} object.
     */
    @SneakyThrows
    public File convertToPdf(File inputFile, File outputDir, Path userProfilePath) {
        String sofficeCommand = config.getLibreoffice().getPath();
        String[] command = {
                sofficeCommand,
                "-env:UserInstallation=file://" + userProfilePath.toAbsolutePath(),
                "--headless",
                "--convert-to", "pdf",
                "--outdir", outputDir.getAbsolutePath(),
                inputFile.getAbsolutePath()
        };

        log.info("Executing LibreOffice conversion for '{}'.", inputFile.getName());
        log.debug("LibreOffice command: {}", String.join(" ", command));
        Process process = new ProcessBuilder(command).start();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?> stdoutFuture = executor.submit(new StreamGobbler(process.getInputStream(), log::debug));
            Future<?> stderrFuture = executor.submit(new StreamGobbler(process.getErrorStream(), log::warn));

            if (!process.waitFor(2, TimeUnit.MINUTES)) {
                process.destroyForcibly();
                log.error("LibreOffice conversion timed out for file: {}", inputFile.getName());
                throw new FileConversionException("LibreOffice conversion timed out for file: " + inputFile.getName());
            }

            stdoutFuture.get(5, TimeUnit.SECONDS);
            stderrFuture.get(5, TimeUnit.SECONDS);

            if (process.exitValue() != 0) {
                log.error("LibreOffice conversion failed for file '{}' with exit code {}. Check warning logs for stderr output.",
                        inputFile.getName(), process.exitValue());
                throw new FileConversionException("LibreOffice conversion failed for file: " + inputFile.getName() + ". Check logs for details.");
            }
        }

        String pdfFileName = FilenameUtils.getBaseName(inputFile.getName()) + ".pdf";
        File pdfFile = new File(outputDir, pdfFileName);

        if (!pdfFile.exists() || pdfFile.length() == 0) {
            log.error("LibreOffice conversion process completed, but the output PDF '{}' was not found or is empty.", pdfFile.getAbsolutePath());
            throw new FileConversionException("Converted PDF file not found or is empty: " + pdfFile.getAbsolutePath());
        }

        log.info("Successfully converted '{}' to PDF at '{}'.", inputFile.getName(), pdfFile.getAbsolutePath());
        return pdfFile;
    }
}