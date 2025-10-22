package com.eyelevel.documentprocessor.service.handlers.impl.msghandler;

import com.eyelevel.documentprocessor.exception.FileConversionException;
import com.lowagie.text.pdf.BaseFont;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.xhtmlrenderer.pdf.ITextRenderer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Service
@Slf4j
@RequiredArgsConstructor
public class HtmlToPdfConverter {

    /**
     * Converts an HTML string to a PDF byte array. This operation includes retry logic
     * for transient system-level failures (like resource starvation causing timeouts or OOMs)
     * but will fail immediately for deterministic data errors.
     *
     * @param htmlContent The HTML to convert.
     * @param contextInfo A string for logging.
     *
     * @return A byte array containing the generated PDF, or null if recovery fails.
     *
     * @throws FileConversionException if a permanent, non-retryable error occurs.
     */
    @Retryable(retryFor = {Exception.class, OutOfMemoryError.class}, noRetryFor = {FileConversionException.class},
               maxAttemptsExpression = "#{${app.processing.msg-handler.retry.attempts} + 1}",
               backoff = @Backoff(delayExpression = "#{${app.processing.msg-handler.retry.delay-ms}}"),
               listeners = {"htmlToPdfRetryListener"})
    public byte[] convertHtmlToPdfBytes(String htmlContent, String contextInfo) throws FileConversionException {
        log.info("[{}] Attempting to convert email body HTML to PDF.", contextInfo);
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            ITextRenderer renderer = new ITextRenderer();

            try {
                renderer.getFontResolver().addFont("fonts/DejaVuSans.ttf", BaseFont.IDENTITY_H, BaseFont.EMBEDDED);
            } catch (IOException e) {
                log.warn("[{}] Font 'fonts/DejaVuSans.ttf' not found. Proceeding with default fonts.", contextInfo);
            }

            renderer.setDocumentFromString(htmlContent);
            renderer.layout();
            renderer.createPDF(os);

            byte[] pdfBytes = os.toByteArray();
            if (pdfBytes.length == 0) {
                // This is a permanent failure. Throw the non-retryable exception.
                throw new FileConversionException("ITextRenderer produced an empty 0-byte PDF.");
            }
            return pdfBytes;
        } catch (Exception e) {
            // This could be a parsing error from ITextRenderer (permanent) or another runtime issue.
            // We wrap it in FileConversionException to signal that it should NOT be retried.
            // If it was a truly transient error, the higher-level retryFor={Exception.class} would have caught it
            // before it was wrapped, but this makes our intent clear.
            log.error("[{}] A terminal exception occurred during HTML to PDF conversion.", contextInfo, e);
            throw new FileConversionException("Failed to convert HTML to PDF: " + e.getMessage(), e);
        }
    }

    /**
     * This method is called by Spring Retry only when all retries for a transient
     * exception (like OOM or a temporary runtime error) have been exhausted.
     */
    @Recover
    public byte[] recover(Throwable e, String htmlContent, String contextInfo) throws FileConversionException {
        String errorMessage = "HTML to PDF conversion failed after all retry attempts due to a persistent system-level issue.";
        log.error("[{}] {}", contextInfo, errorMessage, e);
        // We throw a final exception to signal that the process has ultimately failed.
        throw new FileConversionException(errorMessage, e);
    }
}