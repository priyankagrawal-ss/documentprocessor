package com.eyelevel.documentprocessor.service.file;

import com.eyelevel.documentprocessor.service.handlers.factory.FileHandlerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Slf4j
@Service
@RequiredArgsConstructor
public class ValidationService {

    private final FileHandlerFactory fileHandlerFactory;

    /**
     * Performs full validation on a file, including name, size, and supported type.
     *
     * @param fileName The name of the file, which may include path information.
     * @param fileSize The size of the file in bytes.
     *
     * @return An error message if validation fails, or {@code null} if the file is valid.
     */
    public String validateFileFully(final String fileName, final long fileSize) {
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

        final String extension = FilenameUtils.getExtension(baseName);
        if (!StringUtils.hasText(extension) || fileHandlerFactory.getHandler(extension).isEmpty()) {
            return "File type '" + extension + "' is not supported.";
        }

        log.trace("File '{}' passed all validation checks.", fileName);
        return null;
    }
}
