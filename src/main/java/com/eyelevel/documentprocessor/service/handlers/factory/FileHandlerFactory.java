package com.eyelevel.documentprocessor.service.handlers.factory;

import com.eyelevel.documentprocessor.service.handlers.FileHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * A factory for retrieving the appropriate {@link FileHandler} for a given file extension.
 * It maintains a list of all available handlers and finds the first one that supports the extension.
 */
@Service
@Slf4j
public class FileHandlerFactory {

    private final List<FileHandler> handlers;

    public FileHandlerFactory(List<FileHandler> handlers) {
        this.handlers = handlers;
        log.info("FileHandlerFactory initialized with {} available handlers.", handlers.size());
    }

    /**
     * Finds and returns a FileHandler that supports the specified file extension.
     *
     * @param extension The file extension (e.g., "pdf", "docx").
     * @return An {@link Optional} containing the matched {@link FileHandler}, or empty if no handler is found.
     */
    public Optional<FileHandler> getHandler(String extension) {
        Optional<FileHandler> handler = handlers.stream()
                .filter(h -> h.supports(extension))
                .findFirst();
        log.debug("Searching for handler for extension '{}'. Found: {}", extension, handler.map(h -> h.getClass().getSimpleName()).orElse("None"));
        return handler;
    }
}