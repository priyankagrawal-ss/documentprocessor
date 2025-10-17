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
 * An implementation of {@link FileHandler} that processes ZIP archives by extracting all
 * contained files for further pipeline processing.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ZipHandler implements FileHandler {

    private final ZipContentExtractor zipContentExtractor;

    @Override
    public boolean supports(String extension) {
        return "zip".equalsIgnoreCase(extension);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ExtractedFileItem> handle(InputStream inputStream, FileMaster context) throws IOException, FileConversionException {
        String contextInfo = String.format("JobId: %d, FileMasterId: %d",
                context.getProcessingJob().getId(), context.getId());
        log.info("[{}] ZipHandler processing file '{}'", contextInfo, context.getFileName());

        List<ExtractedFileItem> extractedItems = zipContentExtractor.extract(inputStream, contextInfo);

        log.info("[{}] ZipHandler finished unpacking '{}'. Extracted {} item(s).",
                contextInfo, context.getFileName(), extractedItems.size());
        return extractedItems;
    }
}