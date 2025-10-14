package com.eyelevel.documentprocessor.service.handlers.impl;

import com.eyelevel.documentprocessor.model.ExtractedFileItem;
import com.eyelevel.documentprocessor.model.FileMaster;
import com.eyelevel.documentprocessor.service.handlers.FileHandler;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.poi.hsmf.MAPIMessage;
import org.apache.poi.hsmf.datatypes.AttachmentChunks;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.stereotype.Component;
import org.xhtmlrenderer.pdf.ITextRenderer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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
        log.info("[JobId: {}, FileMasterId: {}] Starting MSG file processing for '{}'.", jobId, fileMasterId, context.getFileName());

        List<ExtractedFileItem> extractedItems = new ArrayList<>();
        Path tempDir = null;
        int attachmentCount = 0;

        try (MAPIMessage msg = new MAPIMessage(inputStream)) {
            for (AttachmentChunks chunk : msg.getAttachmentFiles()) {
                if (chunk.getAttachData() != null && chunk.getAttachData().getValue() != null) {
                    String filename = chunk.getAttachLongFileName() != null ?
                            chunk.getAttachLongFileName().toString() :
                            (chunk.getAttachFileName() != null ? chunk.getAttachFileName().toString() : "attachment-" + UUID.randomUUID());

                    byte[] content = chunk.getAttachData().getValue();
                    extractedItems.add(new ExtractedFileItem(filename, content));
                    attachmentCount++;
                    log.debug("[JobId: {}, FileMasterId: {}] Extracted attachment '{}' ({} bytes).", jobId, fileMasterId, filename, content.length);
                }
            }
            log.info("[JobId: {}, FileMasterId: {}] Extracted {} attachments from MSG file.", jobId, fileMasterId, attachmentCount);

            String cleanHtml = getBodyAsCleanHtml(msg);
            if (!cleanHtml.isBlank()) {
                log.info("[JobId: {}, FileMasterId: {}] Found email body. Converting to PDF.", jobId, fileMasterId);
                tempDir = Files.createTempDirectory("msg-body-pdf-");
                Path tempPdfPath = tempDir.resolve("Email_Body_" + UUID.randomUUID() + ".pdf");
                log.debug("[JobId: {}, FileMasterId: {}] Created temporary directory for PDF body: {}", jobId, fileMasterId, tempDir);

                try (OutputStream os = new FileOutputStream(tempPdfPath.toFile())) {
                    ITextRenderer renderer = new ITextRenderer();
                    renderer.setDocumentFromString(cleanHtml);
                    renderer.layout();
                    renderer.createPDF(os);
                }

                if (Files.exists(tempPdfPath) && Files.size(tempPdfPath) > 0) {
                    byte[] bodyPdfBytes = Files.readAllBytes(tempPdfPath);
                    extractedItems.add(new ExtractedFileItem(tempPdfPath.getFileName().toString(), bodyPdfBytes));
                    log.info("[JobId: {}, FileMasterId: {}] Successfully converted email body to PDF ({} bytes).", jobId, fileMasterId, bodyPdfBytes.length);
                } else {
                    log.warn("[JobId: {}, FileMasterId: {}] Email body conversion to PDF resulted in an empty file.", jobId, fileMasterId);
                }
            } else {
                log.info("[JobId: {}, FileMasterId: {}] No renderable email body found in MSG file.", jobId, fileMasterId);
            }
        } finally {
            if (tempDir != null) {
                log.debug("[JobId: {}, FileMasterId: {}] Cleaning up temporary directory: {}", jobId, fileMasterId, tempDir);
                try {
                    FileUtils.deleteDirectory(tempDir.toFile());
                } catch (IOException e) {
                    log.error("[JobId: {}, FileMasterId: {}] Failed to clean up temporary directory: {}", jobId, fileMasterId, tempDir, e);
                }
            }
        }
        log.info("[JobId: {}, FileMasterId: {}] Finished MSG processing. Produced {} total items.", jobId, fileMasterId, extractedItems.size());
        return extractedItems;
    }

    /**
     * Safely extracts and cleans the email body from the MAPIMessage object.
     * It prioritizes the HTML body and falls back to the plain text body.
     *
     * @param msg The parsed MAPIMessage object.
     * @return A string containing a well-formed HTML document ready for PDF rendering.
     */
    @SneakyThrows
    private String getBodyAsCleanHtml(MAPIMessage msg) {
        String subject = msg.getSubject() != null ? Jsoup.parse(msg.getSubject()).text() : "No Subject";
        String from = msg.getDisplayFrom() != null ? Jsoup.parse(msg.getDisplayFrom()).text() : "Unknown Sender";
        String to = msg.getDisplayTo() != null ? Jsoup.parse(msg.getDisplayTo()).text() : "Undisclosed Recipients";
        String bodyContent;
        String rawHtml = msg.getHtmlBody();

        if (rawHtml != null && !rawHtml.isBlank()) {
            bodyContent = Jsoup.parse(rawHtml).body().html();
        } else if (msg.getTextBody() != null && !msg.getTextBody().isBlank()) {
            String plainText = Jsoup.parse(msg.getTextBody()).text();
            bodyContent = "<pre>" + plainText.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;") + "</pre>";
        } else {
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
        finalDoc.outputSettings().syntax(Document.OutputSettings.Syntax.xml);
        return finalDoc.html();
    }
}