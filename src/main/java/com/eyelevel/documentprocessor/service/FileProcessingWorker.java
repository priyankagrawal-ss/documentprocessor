package com.eyelevel.documentprocessor.service;

import com.eyelevel.documentprocessor.model.FileMaster;
import com.eyelevel.documentprocessor.model.FileProcessingStatus;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * A dedicated service to handle transactional database operations for consumers.
 * This class exists to solve the transactional self-invocation problem by ensuring
 * that transactional methods are called from an external dependency, which allows
 * Spring's AOP proxy to correctly manage the transaction lifecycle.
 */
@Service
@RequiredArgsConstructor
public class FileProcessingWorker {

    private final FileMasterRepository fileMasterRepository;

    /**
     * Atomically finds the next queued file and updates its status to IN_PROGRESS.
     * This "locks" the record so other concurrent workers won't pick it up.
     * This method runs in its own new transaction.
     *
     * @return An Optional containing the locked FileMaster, or empty if none were found.
     */
    @Transactional
    public boolean lockFileForProcessing(Long fileMasterId) {
        Optional<FileMaster> fileOptional = fileMasterRepository.findById(fileMasterId);

        if (fileOptional.isEmpty() || fileOptional.get().getFileProcessingStatus() != FileProcessingStatus.QUEUED) {
            return false;
        }

        FileMaster file = fileOptional.get();
        file.setFileProcessingStatus(FileProcessingStatus.IN_PROGRESS);
        fileMasterRepository.save(file);
        return true;
    }

    /**
     * Atomically updates a file's status to FAILED in case of a critical, unhandled error.
     * This method runs in its own new transaction.
     *
     * @param fileId       The ID of the file to update.
     * @param errorMessage The error message to record.
     */
    @Transactional
    public void revertFileStatusToFailed(Long fileId, String errorMessage) {
        fileMasterRepository.findById(fileId).ifPresent(file -> {
            file.setFileProcessingStatus(FileProcessingStatus.FAILED);
            file.setErrorMessage(errorMessage);
            fileMasterRepository.save(file);
        });
    }
}