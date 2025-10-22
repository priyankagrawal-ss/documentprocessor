package com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.optimizer;

import com.eyelevel.documentprocessor.config.DocumentProcessingConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PdfProcessingStrategyConfig {

    @Bean
    public PdfOptimizer pdfOptimizer(DocumentProcessingConfig config, GhostscriptOptimizer ghostscriptOptimizer,
                                     NoOptimizer noOpOptimizer) {
        if (config.getPdf().isOptimize()) {
            return ghostscriptOptimizer;
        } else {
            return noOpOptimizer;
        }
    }
}