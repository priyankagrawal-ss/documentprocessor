package com.eyelevel.documentprocessor.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Set;

@Data
@ConfigurationProperties(prefix = "app.processing")
public class DocumentProcessingConfig {

    private long maxFileSize;
    private int maxPages;
    private LibreOffice libreoffice = new LibreOffice();
    private Pdf pdf = new Pdf();

    @Data
    public static class LibreOffice {
        public final String path = "soffice";
        public final Set<String> convertibleExtensions = Set.of("doc", "docx", "ppt", "pptx", "xls", "xlsx", "wpd", "rtf",
                "txt", "odt", "ods", "odp");
    }

    @Data
    public static class Pdf {
        private boolean optimize = false;
        private Ghostscript ghostscript = new Ghostscript();

        @Data
        public static class Ghostscript {
            private String preset = "/ebook";
        }
    }
}