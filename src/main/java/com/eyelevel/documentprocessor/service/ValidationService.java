package com.eyelevel.documentprocessor.service;

// Import the FileHandlerFactory

import com.eyelevel.documentprocessor.service.handlers.factory.FileHandlerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * A centralized, stateless service for performing validation checks on files.
 */
@Slf4j
@Service
@RequiredArgsConstructor // Use Lombok to create the constructor
public class ValidationService {

    // Inject the FileHandlerFactory
    private final FileHandlerFactory fileHandlerFactory;

    /**
     * Performs a series of pre-flight checks on a file's name and size to determine if it is
     * eligible for processing.
     *
     * @param fileName The name of the file, which may include path information.
     * @param fileSize The size of the file in bytes.
     * @return An error message string if validation fails, or {@code null} if the file is valid.
     */
    public String validateFile(final String fileName, final long fileSize) {
        log.trace("Validating file '{}' with size {} bytes.", fileName, fileSize);

        if (fileSize <= 0) {
            return "File is empty or has an invalid size.";
        }

        final String baseName = FilenameUtils.getName(fileName);

        if (!StringUtils.hasText(baseName) || baseName.trim().equals(".")) {
            return "File has an invalid or empty name.";
        }

        if (baseName.startsWith(".")) {
            return "File is a hidden file and will be ignored.";
        }

        log.trace("File '{}' passed all validation checks.", fileName);
        return null;
    }

    /**
     * Checks if a given file extension is supported by the document processing pipeline.
     *
     * @param extension The file extension (e.g., "pdf", "docx") without the leading dot.
     * @return {@code true} if a handler exists for this file type, {@code false} otherwise.
     */
    public boolean isFileTypeSupported(final String extension) {
        if (!StringUtils.hasText(extension)) {
            // A file without an extension is considered unsupported.
            return false;
        }
        // The file type is supported if the factory can provide a handler for it.
        boolean isSupported = fileHandlerFactory.getHandler(extension).isPresent();
        log.trace("Checking support for extension '{}': {}", extension, isSupported);
        return isSupported;
    }
}