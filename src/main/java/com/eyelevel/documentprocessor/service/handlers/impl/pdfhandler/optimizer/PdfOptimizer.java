package com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.optimizer;

import com.eyelevel.documentprocessor.exception.FileConversionException;
import com.eyelevel.documentprocessor.exception.FileProtectedException;

import java.io.File;
import java.io.IOException;

/**
 * Strategy interface for optimizing (compressing) a PDF document.
 */
public interface PdfOptimizer {

    /**
     * Optimizes a PDF file.
     *
     * @param inputFile   The file to optimize.
     * @param contextInfo A string for logging.
     *
     * @return The optimized file (may be the original file if no optimization occurred).
     */
    File optimize(File inputFile, String contextInfo)
    throws FileConversionException, FileProtectedException, InterruptedException, IOException;

    PDFOptimizerStrategy getStrategyName();
}
