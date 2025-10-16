package com.eyelevel.documentprocessor.service.handlers.impl.msghandler;

import com.eyelevel.documentprocessor.model.ExtractedFileItem;
import com.eyelevel.documentprocessor.model.FileMaster;
import com.eyelevel.documentprocessor.service.handlers.FileHandler;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.hsmf.MAPIMessage;
import org.apache.poi.hsmf.datatypes.AttachmentChunks;
import org.apache.poi.hsmf.exceptions.ChunkNotFoundException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.safety.Safelist;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * A file handler that processes Microsoft Outlook .msg files, extracting attachments
 * and converting the email body into a separate PDF.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class MsgHandler implements FileHandler {
    private final HtmlToPdfConverter htmlToPdfConverter;

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supports(String extension) {
        boolean isSupported = "msg".equalsIgnoreCase(extension);
        log.trace("Checking MSG support for extension '{}': {}", extension, isSupported);
        return isSupported;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SneakyThrows
    public List<ExtractedFileItem> handle(InputStream inputStream, FileMaster context) {
        long jobId = context.getProcessingJob().getId();
        long fileMasterId = context.getId();
        String contextInfo = String.format("JobId: %d, FileMasterId: %d", jobId, fileMasterId);
        log.info("[{}] Starting MSG file processing for '{}'.", contextInfo, context.getFileName());

        List<ExtractedFileItem> extractedItems = new ArrayList<>();
        int attachmentCount = 0;

        try (MAPIMessage msg = new MAPIMessage(inputStream)) {
            // Attachment extraction logic remains the same
            for (AttachmentChunks chunk : msg.getAttachmentFiles()) {
                if (chunk.getAttachData() != null && chunk.getAttachData().getValue() != null) {
                    String filename = chunk.getAttachLongFileName() != null ?
                            chunk.getAttachLongFileName().toString() :
                            (chunk.getAttachFileName() != null ? chunk.getAttachFileName().toString() : "attachment-" + UUID.randomUUID());

                    byte[] content = chunk.getAttachData().getValue();
                    extractedItems.add(new ExtractedFileItem(filename, content));
                    attachmentCount++;
                }
            }
            log.info("[{}] Extracted {} attachments from MSG file.", contextInfo, attachmentCount);

            String cleanHtml = getBodyAsCleanHtml(msg);
            if (!cleanHtml.isBlank()) {
                log.info("[{}] Found email body. Attempting conversion to PDF.", contextInfo);

                byte[] bodyPdfBytes = htmlToPdfConverter.convertHtmlToPdfBytes(cleanHtml, contextInfo);

                if (bodyPdfBytes != null && bodyPdfBytes.length > 0) {
                    String pdfFileName = "Email_Body_" + UUID.randomUUID() + ".pdf";
                    extractedItems.add(new ExtractedFileItem(pdfFileName, bodyPdfBytes));
                    log.info("[{}] Successfully converted email body to PDF ({} bytes).", contextInfo, bodyPdfBytes.length);
                } else {
                    log.warn("[{}] Email body conversion to PDF resulted in an empty file or failed after retries.", contextInfo);
                }
            } else {
                log.info("[{}] No renderable email body found in MSG file.", contextInfo);
            }
        }
        // The finally block for cleaning temp directories is no longer needed for the PDF part
        log.info("[{}] Finished MSG processing. Produced {} total items.", contextInfo, extractedItems.size());
        return extractedItems;
    }

    /**
     * Safely extracts and cleans the email body from the MAPIMessage object.
     * It prioritizes the HTML body, falls back to the plain text body,
     * and sanitizes the content to make it robust against parsing errors.
     *
     * @param msg The parsed MAPIMessage object.
     * @return A string containing a well-formed HTML document ready for PDF rendering.
     */
    private String getBodyAsCleanHtml(MAPIMessage msg) {
        // --- FIX #2: Robustly get each property, providing a default value if it's missing ---
        String subject = "No Subject";
        try {
            String rawSubject = msg.getSubject();
            if (rawSubject != null) {
                subject = Jsoup.parse(rawSubject).text();
            }
        } catch (ChunkNotFoundException e) {
            log.warn("Subject chunk not found in MSG file. Using default.");
        }

        String from = "Unknown Sender";
        try {
            String rawFrom = msg.getDisplayFrom();
            if (rawFrom != null) {
                from = Jsoup.parse(rawFrom).text();
            }
        } catch (ChunkNotFoundException e) {
            log.warn("DisplayFrom chunk not found in MSG file. Using default.");
        }

        String to = "Undisclosed Recipients";
        try {
            String rawTo = msg.getDisplayTo();
            if (rawTo != null) {
                to = Jsoup.parse(rawTo).text();
            }
        } catch (ChunkNotFoundException e) {
            log.warn("DisplayTo chunk not found in MSG file. Using default.");
        }

        String bodyContent = "";
        try {
            // First, try to get the HTML body
            String rawHtml = msg.getHtmlBody();
            if (rawHtml != null && !rawHtml.isBlank()) {
                // --- FIX #1: Clean the HTML to prevent XML parsing errors ---
                String sanitizedHtml = Jsoup.clean(rawHtml, Safelist.relaxed());
                bodyContent = Jsoup.parse(sanitizedHtml).body().html();
            } else {
                // Fallback to the plain text body if HTML is absent or empty
                throw new ChunkNotFoundException(); // Jump to the text body catch block
            }
        } catch (ChunkNotFoundException e) {
            log.debug("HTML body not found or empty, attempting to fall back to plain text body.");
            try {
                String textBody = msg.getTextBody();
                if (textBody != null && !textBody.isBlank()) {
                    String plainText = Jsoup.parse(textBody).text();
                    // Wrap plain text in <pre> to preserve whitespace and line breaks
                    bodyContent = "<pre>" + plainText.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;") + "</pre>";
                }
            } catch (ChunkNotFoundException e2) {
                log.warn("No renderable HTML or text body found in the MSG file.");
                return ""; // No body content found at all
            }
        }

        // If there's no meaningful content after trying both, return empty.
        if (bodyContent.isBlank()) {
            return "";
        }

        String finalHtml = """
                <html><head><meta charset="UTF-8"><style>body { font-family: sans-serif; }</style></head><body>
                <h2>%s</h2>
                <p><b>From:</b> %s</p>
                <p><b>To:</b> %s</p>
                <hr/>
                %s
                </body></html>
                """.formatted(subject, from, to, bodyContent);

        Document finalDoc = Jsoup.parse(finalHtml);
        return finalDoc.html();
    }
}