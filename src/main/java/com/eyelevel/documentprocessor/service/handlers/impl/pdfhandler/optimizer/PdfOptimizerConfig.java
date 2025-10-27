package com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.optimizer;

import com.eyelevel.documentprocessor.config.DocumentProcessingConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration class responsible for providing the active {@link PdfOptimizer} implementation
 * based on application configuration.
 *
 * <p>This allows dynamic selection of optimizer strategy (e.g. QPDF, Ghostscript, or No-op)
 * without changing code. The selected optimizer is exposed as a Spring bean named {@code pdfOptimizer}.
 *
 * <h3>Example configuration (application.yml):</h3>
 * <pre>
 * app:
 *   processing:
 *     pdf:
 *       optimizer-strategy: qpdf        # or ghostscript, none
 * </pre>
 *
 * <p>Available strategies:
 * <ul>
 *   <li>{@code qpdf} → {@link com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.optimizer.qpdf.QPDFOptimizer}</li>
 *   <li>{@code ghostscript} → {@link com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.optimizer.gs.GhostscriptOptimizer}</li>
 *   <li>{@code none} → {@link com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.optimizer.noop.NoPDFOptimizer}</li>
 * </ul>
 */
@Configuration
@Slf4j
public class PdfOptimizerConfig {

    /**
     * Creates a {@link PdfOptimizer} bean named {@code pdfOptimizer} according to the configuration.
     *
     * @param qpdf   QPDF-based optimizer
     * @param gs     Ghostscript-based optimizer
     * @param noop   No-operation optimizer (returns input as-is)
     * @param config Loaded document processing configuration
     * @return the selected {@link PdfOptimizer} bean
     */
    @Bean(name = "pdfOptimizer")
    public PdfOptimizer pdfOptimizer(
            @Qualifier("qpdfOptimizer") PdfOptimizer qpdf,
            @Qualifier("ghostscriptOptimizer") PdfOptimizer gs,
            @Qualifier("noPdfOptimizer") PdfOptimizer noop,
            DocumentProcessingConfig config) {

        String strategy = config.getPdf().getOptimizerStrategy();

        if (strategy == null || strategy.isBlank()) {
            log.warn("No PDF optimizer strategy configured. Defaulting to 'none' (no optimization).");
            return noop;
        }

        log.info("Initializing PDF Optimizer configuration. Selected strategy: '{}'", strategy);

        return switch (strategy.trim().toLowerCase()) {
            case "qpdf" -> {
                log.info("PDF optimization strategy set to QPDF (using qpdf command-line tool).");
                yield qpdf;
            }
            case "ghostscript" -> {
                log.info("PDF optimization strategy set to Ghostscript (using gs command-line tool).");
                yield gs;
            }
            case "none" -> {
                log.info("PDF optimization strategy set to NONE (optimization disabled).");
                yield noop;
            }
            default -> {
                log.error("Unknown PDF optimizer strategy '{}'. Falling back to 'none'.", strategy);
                yield noop;
            }
        };
    }
}
