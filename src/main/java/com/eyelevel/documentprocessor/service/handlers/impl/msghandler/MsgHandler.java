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

// In MsgHandler.java

    /**
     * Safely extracts and cleans the email body from the MAPIMessage object.
     * It prioritizes the HTML body, falls back to the plain text body,
     * and produces a well-formed, correctly styled XHTML string suitable for PDF rendering.
     *
     * @param msg The parsed MAPIMessage object.
     * @return A string containing a well-formed XHTML document.
     */
    private String getBodyAsCleanHtml(MAPIMessage msg) {
        // This part of the code is correct and does not need to change.
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
            // First, try to get the rich HTML body
            String rawHtml = msg.getHtmlBody();
            if (rawHtml != null && !rawHtml.isBlank()) {
                Safelist safelist = Safelist.relaxed()
                        .addAttributes(":all", "style", "class", "id")
                        .addTags("table", "thead", "tbody", "tfoot", "tr", "th", "td", "div", "span")
                        .addAttributes("table", "summary", "width", "cellpadding", "cellspacing")
                        .addAttributes("td", "abbr", "axis", "colspan", "rowspan", "width", "valign")
                        .addAttributes("th", "abbr", "axis", "colspan", "rowspan", "scope", "width", "valign");
                String sanitizedHtml = Jsoup.clean(rawHtml, safelist);
                bodyContent = Jsoup.parse(sanitizedHtml).body().html();
            } else {
                // If no HTML body, trigger the fallback to plain text
                throw new ChunkNotFoundException();
            }
        } catch (ChunkNotFoundException e) {
            log.debug("HTML body not found or empty, attempting to fall back to plain text body.");
            try {
                // --- THIS IS THE CRITICAL FIX FOR THE "WALL OF TEXT" PROBLEM ---
                String plainTextBody = msg.getTextBody();
                if (plainTextBody != null && !plainTextBody.isBlank()) {
                    // 1. Get the raw text WITH its original newline characters. DO NOT use Jsoup.text().
                    // 2. Escape any special HTML characters to prevent breaking the structure.
                    String escapedText = plainTextBody.replace("&", "&amp;")
                            .replace("<", "&lt;")
                            .replace(">", "&gt;");
                    // 3. Wrap the result in a <pre> tag. The newlines are preserved, and the
                    //    CSS 'white-space: pre-wrap' will handle the formatting.
                    bodyContent = "<pre>" + escapedText + "</pre>";
                } else {
                    return ""; // No body content at all
                }
                // --------------------------------------------------------------------
            } catch (ChunkNotFoundException e2) {
                log.warn("No renderable HTML or text body found in the MSG file.");
                return "";
            }
        }

        if (bodyContent.isBlank()) {
            return "";
        }

        String finalHtml = """
                <html>
                  <head>
                    <meta charset="UTF-8"/>
                    <style>
                      body {
                        font-family: sans-serif;
                        overflow-wrap: break-word;
                        word-wrap: break-word;
                      }
                      pre {
                        white-space: pre-wrap; /* Preserves whitespace/newlines AND wraps long lines */
                        overflow-wrap: break-word;
                        word-wrap: break-word;
                        font-family: sans-serif; /* Use the same font as the body */
                      }
                    </style>
                  </head>
                  <body>
                    <h2>%s</h2>
                    <p><b>From:</b> %s</p>
                    <p><b>To:</b> %s</p>
                    <hr/>
                    %s
                  </body>
                </html>
                """.formatted(subject, from, to, bodyContent);

        Document finalDoc = Jsoup.parse(finalHtml);
        finalDoc.outputSettings().syntax(Document.OutputSettings.Syntax.xml);
        return finalDoc.html();
    }
}