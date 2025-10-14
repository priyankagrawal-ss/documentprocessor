package com.eyelevel.documentprocessor.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Set;

/**
 * Binds application properties under the "app.processing" prefix to a strongly-typed
 * configuration object. This provides centralized control over file processing behaviors,
 * such as size limits, page limits, and external tool configurations.
 */
@Data
@ConfigurationProperties(prefix = "app.processing")
public class DocumentProcessingConfig {

    /**
     * The maximum file size in bytes that will be processed. Files larger than this
     * may be split or rejected.
     */
    private long maxFileSize;

    /**
     * The maximum number of pages a PDF can have before it is split into smaller chunks.
     */
    private int maxPages;

    /**
     * Configuration settings for the LibreOffice integration.
     */
    private LibreOffice libreoffice = new LibreOffice();

    /**
     * Configuration settings related to PDF file handling, including optimization.
     */
    private Pdf pdf = new Pdf();

    /**
     * Contains settings specific to the LibreOffice document conversion process.
     */
    @Data
    public static class LibreOffice {
        /**
         * The command-line path to the LibreOffice executable (e.g., "soffice" or "/usr/bin/soffice").
         */
        private String path = "soffice";

        /**
         * A set of file extensions (case-insensitive) that are supported for conversion to PDF
         * by the LibreOfficeHandler.
         */
        private Set<String> convertibleExtensions = Set.of(
                "doc", "docx", "ppt", "pptx", "xls", "xlsx", "wpd", "rtf",
                "txt", "odt", "ods", "odp"
        );
    }

    /**
     * Contains settings specific to PDF processing, such as optimization.
     */
    @Data
    public static class Pdf {
        /**
         * A flag to enable or disable PDF size optimization via Ghostscript.
         */
        private boolean optimize = false;

        /**
         * Configuration for the Ghostscript tool used for PDF optimization.
         */
        private Ghostscript ghostscript = new Ghostscript();

        /**
         * Contains settings for the Ghostscript command-line tool.
         */
        @Data
        public static class Ghostscript {
            /**
             * The Ghostscript preset to use for PDF optimization (e.g., /ebook, /printer, /screen).
             * The /ebook preset provides a good balance of quality and size reduction.
             */
            private String preset = "/ebook";
        }
    }
}