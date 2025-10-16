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

@Service
@Slf4j
@RequiredArgsConstructor
public class HtmlToPdfConverter {

    /**
     * Converts an HTML string to a PDF byte array, with retry logic.
     * This method will retry on any Exception, which is suitable for potential
     * rendering or memory issues in ITextRenderer.
     *
     * @param htmlContent The well-formed HTML to convert.
     * @param contextInfo A string for logging context (e.g., "JobId: 123, FileMasterId: 456").
     * @return A byte array containing the generated PDF.
     * @throws FileConversionException if conversion fails after all retries.
     */
    @Retryable(
            retryFor = {Exception.class}, // Retry on a broad range of potential rendering errors
            maxAttemptsExpression = "#{${app.processing.msg-handler.retry.attempts} + 1}",
            backoff = @Backoff(delayExpression = "#{${app.processing.msg-handler.retry.delay-ms}}"),
            listeners = {"htmlToPdfRetryListener"} // Optional: for logging
    )
    public byte[] convertHtmlToPdfBytes(String htmlContent, String contextInfo) throws FileConversionException {
        log.info("[{}] Attempting to convert email body HTML to PDF.", contextInfo);
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            ITextRenderer renderer = new ITextRenderer();
            renderer.getFontResolver().addFont("fonts/DejaVuSans.ttf", BaseFont.IDENTITY_H, BaseFont.EMBEDDED);
            renderer.setDocumentFromString(htmlContent);
            renderer.layout();
            renderer.createPDF(os);

            byte[] pdfBytes = os.toByteArray();
            if (pdfBytes.length == 0) {
                // This is a failure condition, throw exception to trigger retry
                throw new FileConversionException("ITextRenderer produced an empty 0-byte PDF.");
            }
            return pdfBytes;
        } catch (Exception e) {
            log.error("[{}] Exception during HTML to PDF conversion.", contextInfo, e);
            // Re-throw as a specific exception type to ensure retry logic catches it
            throw new FileConversionException("Failed to convert HTML to PDF: " + e.getMessage(), e);
        }
    }

    /**
     * This method is called by Spring Retry only when all retry attempts for
     * convertHtmlToPdfBytes have failed.
     *
     * @param e           The final exception that caused the failure.
     * @param htmlContent The original HTML content.
     * @param contextInfo The logging context.
     * @return null, indicating failure to convert but allowing the parent process to continue.
     */
    @Recover
    public byte[] recover(FileConversionException e, String htmlContent, String contextInfo) {
        log.error("[{}] HTML to PDF conversion failed after all retry attempts. No PDF will be generated for the email body.", contextInfo, e);
        return null; // Gracefully fail, returning null
    }
}