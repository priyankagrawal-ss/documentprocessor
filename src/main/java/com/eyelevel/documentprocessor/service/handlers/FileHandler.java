package com.eyelevel.documentprocessor.service.handlers;

import com.eyelevel.documentprocessor.model.ExtractedFileItem;
import com.eyelevel.documentprocessor.model.FileMaster;
import org.jodconverter.core.office.OfficeException;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Defines the contract for a file handler within the document processing pipeline.
 * Each implementation is responsible for processing a specific file type.
 */
public interface FileHandler {

    /**
     * Determines if this handler can process a file with the given extension.
     *
     * @param extension The file extension (e.g., "pdf", "docx").
     * @return {@code true} if the handler supports the extension, {@code false} otherwise.
     */
    boolean supports(String extension);

    /**
     * Processes the given input stream for a file.
     * <p>
     * Implementations can either transform the file in place (e.g., Office-to-PDF)
     * or extract multiple new files from it (e.g., from a ZIP or MSG archive).
     *
     * @param inputStream The input stream of the file content.
     * @param context     The database entity representing the file being processed, providing job context.
     * @return A list of {@link ExtractedFileItem} objects.
     * - If new files were extracted, the list will contain them.
     * - If the original file was transformed, the list will contain the single transformed result.
     * - If no transformation or extraction occurred (e.g., a simple optimization), the list will be empty.
     */
    List<ExtractedFileItem> handle(InputStream inputStream, FileMaster context) throws IOException, OfficeException;
}