package com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.splitter;

import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.stereotype.Component;

import java.io.File;

@Component("qpdfRetryListener")
@Slf4j
public class QPDFRetryListener implements RetryListener {
    @Override
    public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback,
                                                 Throwable throwable) {
        if (context.getRetryCount() > 0) {

            String fileName = "UnknownFile";
            Object[] args = (Object[]) context.getAttribute("context.args");

            if (args != null && args.length > 0 && args[0] instanceof File) {
                fileName = ((File) args[0]).getName();
            }

            log.warn("QPDF operation for file '{}' failed on attempt {}. Retrying...", fileName,
                     context.getRetryCount(), throwable);
        }
    }
}
