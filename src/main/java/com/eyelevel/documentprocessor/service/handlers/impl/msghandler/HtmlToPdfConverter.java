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
     * Converts an HTML string to a PDF byte array with retry logic for transient errors.
     * It will gracefully handle a missing font file without failing the conversion.
     *
     * @param htmlContent The well-formed HTML to convert.
     * @param contextInfo A string for logging context (e.g., "JobId: 123, FileMasterId: 456").
     * @return A byte array containing the generated PDF.
     * @throws FileConversionException if a non-recoverable conversion error occurs.
     */
    @Retryable(
            // We only want to retry on non-IO exceptions, like rendering or memory errors.
            retryFor = {Exception.class},
            noRetryFor = {IOException.class}, // Exclude font-not-found errors from retries.
            maxAttemptsExpression = "#{${app.processing.msg-handler.retry.attempts} + 1}",
            backoff = @Backoff(delayExpression = "#{${app.processing.msg-handler.retry.delay-ms}}"),
            listeners = {"htmlToPdfRetryListener"}
    )
    public byte[] convertHtmlToPdfBytes(String htmlContent, String contextInfo) throws FileConversionException {
        log.info("[{}] Attempting to convert email body HTML to PDF.", contextInfo);
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            ITextRenderer renderer = new ITextRenderer();

            // --- THIS IS THE ROBUST FONT HANDLING LOGIC ---
            try {
                // Attempt to add the enhanced font for better character support.
                renderer.getFontResolver().addFont("fonts/DejaVuSans.ttf", BaseFont.IDENTITY_H, BaseFont.EMBEDDED);
            } catch (IOException e) {
                // If the font is not found, log a warning and continue.
                // The renderer will fall back to its default fonts.
                log.warn("[{}] Font 'fonts/DejaVuSans.ttf' not found. Proceeding with default fonts. PDF may not render all characters correctly.", contextInfo);
            }
            // --------------------------------------------------

            renderer.setDocumentFromString(htmlContent);
            renderer.layout();
            renderer.createPDF(os);

            byte[] pdfBytes = os.toByteArray();
            if (pdfBytes.length == 0) {
                // This is still a failure condition we want to retry (might be a transient memory issue).
                throw new FileConversionException("ITextRenderer produced an empty 0-byte PDF.");
            }
            return pdfBytes;
        } catch (Exception e) {
            // This will now only catch other exceptions, which are worth retrying.
            log.error("[{}] A retriable exception occurred during HTML to PDF conversion.", contextInfo, e);
            throw new FileConversionException("Failed to convert HTML to PDF: " + e.getMessage(), e);
        }
    }

    /**
     * This method is called by Spring Retry only when all retries have failed for a
     * retriable exception (e.g., a rendering error).
     */
    @Recover
    public byte[] recover(FileConversionException e, String htmlContent, String contextInfo) {
        log.error("[{}] HTML to PDF conversion failed after all retry attempts. No PDF will be generated for the email body.", contextInfo, e);
        return null; // Gracefully fail, returning null.
    }
}