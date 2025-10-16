package com.eyelevel.documentprocessor.service.handlers.impl.libreoffice;

import com.eyelevel.documentprocessor.exception.FileConversionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jodconverter.core.office.OfficeException;
import org.jodconverter.local.JodConverter;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.io.File;

@Service
@Slf4j
@RequiredArgsConstructor
public class LibreOfficeConverterService {

    private final JodConverter converter;

    /**
     * Converts a source file to a target PDF file using a managed LibreOffice process.
     * This method is retryable and will attempt conversion multiple times on failure.
     *
     * @param inputFile   The source file to convert.
     * @param outputFile  The destination file for the PDF output.
     * @param contextInfo A string for logging context (e.g., "JobId: 123, FileMasterId: 456").
     * @throws FileConversionException if the conversion fails after all retry attempts.
     */
    @Retryable(
            retryFor = {OfficeException.class, FileConversionException.class},
            maxAttemptsExpression = "#{${app.processing.libreoffice.retry.attempts} + 1}",
            backoff = @Backoff(delayExpression = "#{${app.processing.libreoffice.retry.delay-ms}}"),
            listeners = {"libreOfficeRetryListener"} // Optional: for logging
    )
    public void convertToPdf(File inputFile, File outputFile, String contextInfo) throws FileConversionException, OfficeException {
        log.info("[{}] Attempting LibreOffice conversion for '{}'.", contextInfo, inputFile.getName());

        converter.convert(inputFile).to(outputFile).execute();

        if (!outputFile.exists() || outputFile.length() == 0) {
            log.error("[{}] Conversion resulted in a missing or empty file for '{}'.", contextInfo, inputFile.getName());
            // Throw an exception to trigger a retry
            throw new FileConversionException("Conversion resulted in an empty or missing file for: " + inputFile.getName());
        }
    }

    /**
     * This method is called by Spring Retry only when all retry attempts for convertToPdf have failed.
     *
     * @param e           The final exception that caused the failure.
     * @param inputFile   The original input file.
     * @param outputFile  The intended output file.
     * @param contextInfo The logging context.
     * @throws FileConversionException A final, definitive exception indicating failure.
     */
    @Recover
    public void recover(Exception e, File inputFile, File outputFile, String contextInfo) throws FileConversionException {
        String errorMessage = String.format("LibreOffice conversion failed for '%s' after all retry attempts.", inputFile.getName());
        log.error("[{}] {}", contextInfo, errorMessage, e);
        // Throw a final exception to ensure the pipeline fails for this file.
        throw new FileConversionException(errorMessage, e);
    }
}