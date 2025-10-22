package com.eyelevel.documentprocessor.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Set;

/**
 * Binds application properties under the "app.processing" prefix to a strongly-typed
 * configuration object. This provides centralized control over file processing behaviors.
 */
@Data
@ConfigurationProperties(prefix = "app.processing")
public class DocumentProcessingConfig {

    private long maxFileSize;
    private int maxPages;
    private LibreOffice libreoffice = new LibreOffice();
    private Pdf pdf = new Pdf();
    private MsgHandler msgHandler = new MsgHandler();
    private ZipHandler zipHandler = new ZipHandler();


    @Data
    public static class RetryConfig {
        private int attempts;
        private long delayMs;
    }

    @Data
    public static class GhostscriptRetryConfig {
        private RetryConfig retry = new RetryConfig();
    }

    @Data
    public static class MsgHandler {
        private RetryConfig retry = new RetryConfig();
    }

    @Data
    public static class ZipHandler {
        private int concurrencyLimit;
        private String tempDir;
        private RetryConfig retry = new RetryConfig();
    }

    @Data
    public static class LibreOffice {
        private RetryConfig retry = new RetryConfig();

        private Set<String> convertibleExtensions = Set.of("doc", "docx", "ppt", "pptx", "xls", "xlsx", "wpd", "rtf",
                                                           "txt", "odt", "ods", "odp");
    }

    @Data
    public static class Pdf {
        private boolean optimize = true;
        private Ghostscript ghostscript = new Ghostscript();
        private QPDF qpdf = new QPDF();

        @Data
        public static class QPDF {
            private RetryConfig retry = new RetryConfig();
        }

        @Data
        public static class Ghostscript {
            private String preset;
            private long optimizationTimeoutMinutes;
            private RetryConfig retry = new RetryConfig();
        }
    }
}