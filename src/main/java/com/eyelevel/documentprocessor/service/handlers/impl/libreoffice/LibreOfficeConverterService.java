package com.eyelevel.documentprocessor.service.handlers.impl.libreoffice;

import com.eyelevel.documentprocessor.exception.FileConversionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jodconverter.core.office.OfficeException;
import org.jodconverter.core.office.OfficeManager;
import org.jodconverter.local.LocalConverter;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.io.File;

@Service
@Slf4j
@RequiredArgsConstructor
public class LibreOfficeConverterService {

    private final OfficeManager officeManager;

    /**
     * Converts a source file to a target PDF file using a managed LibreOffice process.
     */
    @Retryable(retryFor = {OfficeException.class, FileConversionException.class},
               maxAttemptsExpression = "#{${app.processing.libreoffice.retry.attempts} + 1}",
               backoff = @Backoff(delayExpression = "#{${app.processing.libreoffice.retry.delay-ms}}"),
               listeners = {"libreOfficeRetryListener"})
    public void convertToPdf(File inputFile, File outputFile, String contextInfo) throws FileConversionException {
        log.info("[{}] Attempting LibreOffice conversion for '{}'.", contextInfo, inputFile.getName());

        try {
            // Use LocalConverter with the injected OfficeManager
            LocalConverter.make(officeManager).convert(inputFile).to(outputFile).execute();

            if (!outputFile.exists() || outputFile.length() == 0) {
                log.error("[{}] Conversion resulted in a missing or empty file for '{}'.", contextInfo,
                          inputFile.getName());
                throw new FileConversionException(
                        "Conversion resulted in an empty or missing file for: " + inputFile.getName());
            }

        } catch (OfficeException e) {
            log.error("[{}] LibreOffice conversion failed for '{}'.", contextInfo, inputFile.getName(), e);
            throw new FileConversionException("LibreOffice conversion failed for: " + inputFile.getName(), e);
        }
    }

    @Recover
    public void recover(Exception e, File inputFile, File outputFile, String contextInfo)
    throws FileConversionException {
        String errorMessage = String.format("LibreOffice conversion failed for '%s' after all retry attempts.",
                                            inputFile.getName());
        log.error("[{}] {}", contextInfo, errorMessage, e);
        throw new FileConversionException(errorMessage, e);
    }
}
