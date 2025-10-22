package com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.splitter;

import com.eyelevel.documentprocessor.exception.FileConversionException;
import com.eyelevel.documentprocessor.exception.FileProtectedException;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Strategy interface for splitting a PDF document.
 */
public interface PdfSplitter {

    List<File> split(File inputFile, int pagesPerChunk, String contextInfo)
    throws FileConversionException, FileProtectedException, InterruptedException, IOException;

    int getPageCount(File pdfFile, String contextInfo)
    throws FileConversionException, FileProtectedException, IOException, InterruptedException;

    PDFSplitterStrategy getStrategyName();
}
