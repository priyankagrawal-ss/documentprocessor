package com.eyelevel.documentprocessor.service.asynctask;

import com.eyelevel.documentprocessor.service.s3.S3StorageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

/**
 * A centralized manager for scheduling tasks that should run asynchronously
 * only *after* a database transaction has successfully committed. This service
 * abstracts away the complexity of TransactionSynchronizationManager and uses a
 * managed thread pool for executing callbacks.
 */
@Slf4j
@Service
public class AsyncTaskManager {

    private final S3StorageService s3StorageService;
    private final AsyncTaskExecutor taskExecutor;

    public AsyncTaskManager(S3StorageService s3StorageService,
                            @Qualifier("applicationTaskExecutor") AsyncTaskExecutor taskExecutor) {
        this.s3StorageService = s3StorageService;
        this.taskExecutor = taskExecutor;
    }

    /**
     * Schedules an asynchronous S3 upload from a temporary Path. The upload
     * will only begin after the current transaction commits.
     *
     * @param entityId     The ID of the database entity associated with this upload.
     * @param s3Key        The destination S3 key.
     * @param tempFilePath The Path to the temporary file to upload.
     * @param action       The strategy object defining what to do on upload success or failure.
     */
    public void scheduleUploadAfterCommit(
            final Long entityId,
            final String s3Key,
            final Path tempFilePath,
            final PostUploadAction action
    ) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                log.info("DB transaction committed for entity ID: {}. Starting async S3 upload from path: {}", entityId, tempFilePath);
                try (InputStream inputStream = Files.newInputStream(tempFilePath)) {

                    CompletableFuture<Void> uploadFuture = s3StorageService.uploadAsync(
                            s3Key, inputStream
                    );

                    uploadFuture.thenRunAsync(() -> action.onUploadSuccess(entityId), taskExecutor)
                            .exceptionally(ex -> {
                                action.onUploadFailure(entityId, ex.getCause());
                                return null;
                            });

                } catch (IOException e) {
                    log.error("Failed to open input stream for temp file {}. Triggering failure callback immediately.", tempFilePath, e);
                    action.onUploadFailure(entityId, e);
                }
            }
        });
    }

    /**
     * Schedules an asynchronous S3 upload from a byte array. The upload
     * will only begin after the current transaction commits.
     *
     * @param entityId The ID of the database entity associated with this upload.
     * @param s3Key    The destination S3 key.
     * @param content  The byte array content to upload.
     * @param action   The strategy object defining what to do on upload success or failure.
     */
    public void scheduleUploadAfterCommit(
            final Long entityId,
            final String s3Key,
            final byte[] content,
            final PostUploadAction action
    ) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                log.info("DB transaction committed for entity ID: {}. Starting async S3 upload from byte array to key: {}", entityId, s3Key);

                CompletableFuture<Void> uploadFuture = s3StorageService.uploadAsync(
                        s3Key, new ByteArrayInputStream(content)
                );

                uploadFuture.thenRunAsync(() -> action.onUploadSuccess(entityId), taskExecutor)
                        .exceptionally(ex -> {
                            action.onUploadFailure(entityId, ex.getCause());
                            return null;
                        });
            }
        });
    }
}