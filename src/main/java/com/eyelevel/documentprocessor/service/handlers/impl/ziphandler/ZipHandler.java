package com.eyelevel.documentprocessor.service.handlers.impl.ziphandler;

import com.eyelevel.documentprocessor.exception.FileConversionException;
import com.eyelevel.documentprocessor.model.ExtractedFileItem;
import com.eyelevel.documentprocessor.model.FileMaster;
import com.eyelevel.documentprocessor.service.handlers.FileHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * A file handler that processes ZIP archives by extracting all contained files.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ZipHandler implements FileHandler {

    /**
     * {@inheritDoc}
     */
    private final ZipExtractorService zipExtractorService;

    @Override
    public boolean supports(String extension) {
        return "zip".equalsIgnoreCase(extension);
    }

    @Override
    public List<ExtractedFileItem> handle(InputStream inputStream, FileMaster context) throws IOException, FileConversionException {
        long jobId = context.getProcessingJob().getId();
        long fileMasterId = context.getId();
        String contextInfo = String.format("JobId: %d, FileMasterId: %d", jobId, fileMasterId);
        log.info("[{}] Starting ZIP file unpacking for '{}'.", contextInfo, context.getFileName());

        // --- DELEGATE THE ENTIRE EXTRACTION LOGIC ---
        // The retry logic is now handled automatically by Spring.
        List<ExtractedFileItem> extractedItems = zipExtractorService.extract(inputStream, contextInfo);

        log.info("[{}] Finished unpacking ZIP file. Extracted {} items.", contextInfo, extractedItems.size());
        return extractedItems;
    }
}