package com.eyelevel.documentprocessor.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Set;

/**
 * Binds application properties under the "app.processing" prefix to a strongly-typed
 * configuration object. This provides centralized control over file processing behaviors.
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "app.processing")
public class DocumentProcessingConfig {

    @Data
    public static class RetryConfig {
        private int attempts = 2;
        private long delayMs = 2000;
    }

    @Data
    public static class GhostscriptRetryConfig {
        private int attempts = 2;
        private long delaySeconds = 10;
    }

    private long maxFileSize;
    private int maxPages;
    private LibreOffice libreoffice = new LibreOffice();
    private Pdf pdf = new Pdf();
    private MsgHandler msgHandler = new MsgHandler();

    private ZipHandler zipHandler = new ZipHandler();

    @Data
    public static class MsgHandler {
        private RetryConfig retry = new RetryConfig();
    }

    @Data
    public static class ZipHandler {
        private RetryConfig retry = new RetryConfig();
    }

    @Data
    public static class LibreOffice {
        private RetryConfig retry = new RetryConfig();

        private Set<String> convertibleExtensions = Set.of(
                "doc", "docx", "ppt", "pptx", "xls", "xlsx", "wpd", "rtf",
                "txt", "odt", "ods", "odp"
        );
    }

    @Data
    public static class Pdf {
        private boolean optimize = true;
        private Ghostscript ghostscript = new Ghostscript();

        @Data
        public static class Ghostscript {
            private String preset = "/ebook";
            private long optimizationTimeoutMinutes = 5;
            private GhostscriptRetryConfig retry = new GhostscriptRetryConfig();
        }
    }
}