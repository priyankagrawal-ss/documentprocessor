package com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.optimizer;

import org.springframework.stereotype.Service;

import java.io.File;

@Service
public class NoOptimizer implements PdfOptimizer {
    @Override
    public File optimize(File inputFile, String contextInfo) {
        return inputFile; // Does nothing, just returns the input.
    }

    @Override
    public PDFOptimizerStrategy getStrategyName() {
        return PDFOptimizerStrategy.NO_OPTIMIZER;
    }
}
