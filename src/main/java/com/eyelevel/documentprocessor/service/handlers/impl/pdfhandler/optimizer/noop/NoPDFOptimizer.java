package com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.optimizer.noop;

import com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.optimizer.PDFOptimizerStrategy;
import com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.optimizer.PdfOptimizer;
import org.springframework.stereotype.Service;

import java.io.File;

@Service("noPdfOptimizer")
public class NoPDFOptimizer implements PdfOptimizer {
    @Override
    public File optimize(File inputFile, String contextInfo) {
        return inputFile; // Does nothing, just returns the input.
    }

    @Override
    public PDFOptimizerStrategy getStrategyName() {
        return PDFOptimizerStrategy.NO_OPTIMIZER;
    }
}
