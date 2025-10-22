package com.eyelevel.documentprocessor.service.handlers.impl.libreoffice;

import com.eyelevel.documentprocessor.config.DocumentProcessingConfig;
import com.eyelevel.documentprocessor.exception.FileConversionException;
import com.eyelevel.documentprocessor.model.ExtractedFileItem;
import com.eyelevel.documentprocessor.model.FileMaster;
import com.eyelevel.documentprocessor.service.handlers.FileHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.jodconverter.core.office.OfficeException;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class LibreOfficeHandler implements FileHandler {

    private final DocumentProcessingConfig config;
    private final LibreOfficeConverterService converterService;

    @Override
    public boolean supports(String extension) {
        boolean isSupported = config.getLibreoffice().getConvertibleExtensions().contains(extension.toLowerCase());
        log.trace("Checking LibreOffice support for extension '{}': {}", extension, isSupported);
        return isSupported;
    }

    @Override
    public List<ExtractedFileItem> handle(InputStream inputStream, FileMaster context)
    throws IOException, FileConversionException, OfficeException {
        long jobId = context.getProcessingJob().getId();
        long fileMasterId = context.getId();
        String fileName = context.getFileName();
        String contextInfo = String.format("JobId: %d, FileMasterId: %d", jobId, fileMasterId);
        log.info("[{}] Starting LibreOffice conversion for '{}'.", contextInfo, fileName);

        Path taskTempDir = Files.createTempDirectory("lo-task-" + fileMasterId + "-");
        log.debug("[{}] Created temporary directory: {}", contextInfo, taskTempDir);

        try {
            File inputFile = taskTempDir.resolve(fileName).toFile();
            Files.copy(inputStream, inputFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            String pdfFileName = FilenameUtils.getBaseName(fileName) + ".pdf";
            File outputFile = taskTempDir.resolve(pdfFileName).toFile();

            log.debug("[{}] Delegating conversion for '{}' to retryable service.", contextInfo, inputFile.getName());

            converterService.convertToPdf(inputFile, outputFile, contextInfo);

            log.info("[{}] Successfully converted file. Reading PDF bytes.", fileName);
            byte[] pdfBytes = Files.readAllBytes(outputFile.toPath());

            return Collections.singletonList(new ExtractedFileItem(outputFile.getName(), pdfBytes));

        } finally {
            log.debug("[{}] Cleaning up temporary directory: {}", contextInfo, taskTempDir);
            FileUtils.deleteDirectory(taskTempDir.toFile());
        }
    }
}