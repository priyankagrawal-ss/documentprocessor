package com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class GhostscriptRetryListener implements RetryListener {
    @Override
    public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
        if (context.getRetryCount() > 0) {
            log.warn("Ghostscript optimization failed on attempt {}. Retrying...", context.getRetryCount(), throwable);
        }
    }
}