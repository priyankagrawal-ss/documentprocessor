package com.eyelevel.documentprocessor.service.handlers.impl.pdfhandler.optimizer.qpdf;

import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.stereotype.Component;

import java.io.File;

@Slf4j
@Component("qpdfOptimizerRetryListener")
public class QPDFOptimizerRetryListener implements RetryListener {

    /**
     * Called after a failed attempt.
     *
     * @param context   The current retry context.
     * @param callback  The callback that was executed.
     * @param throwable The exception that was thrown.
     */
    @Override
    public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
        // The arguments are extracted from the RetryCallback's label, which is set by Spring's AOP interceptor.
        // We expect the arguments of the 'optimize' method: (File inputFile, String contextInfo)
        Object[] args = context.attributeNames();

        String contextInfo = "Unknown Context";
        String fileName = "Unknown File";

        // Extract arguments to prevent class cast exceptions
        if (args.length >= 2) {
            if (args[0] instanceof File) {
                fileName = ((File) args[0]).getName();
            }
            if (args[1] instanceof String) {
                contextInfo = (String) args[1];
            }
        }

        log.warn("[{}] QPDF optimization for '{}' failed on attempt {}. Retrying... Error: {}",
                contextInfo,
                fileName,
                context.getRetryCount(),
                throwable.getMessage());
    }

    /**
     * Called before the first attempt.
     * We don't need custom logic here, so it's a no-op.
     */
    @Override
    public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
        // Return true to signal that the retry operation should proceed.
        return true;
    }

    /**
     * Called after the final attempt (successful or not).
     * We don't need custom logic here, so it's a no-op.
     */
    @Override
    public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
        // No action needed on close.
    }
}