package com.eyelevel.documentprocessor.service;

import com.eyelevel.documentprocessor.model.FileMaster;
import com.eyelevel.documentprocessor.model.FileProcessingStatus;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * A dedicated service to handle the transactional "locking" of a file record.
 * This prevents multiple concurrent consumers from processing the same file.
 */
@Service
@RequiredArgsConstructor
public class FileLockingService {

    private final FileMasterRepository fileMasterRepository;

    /**
     * Atomically finds a file by its ID and updates its status from {@code QUEUED} to {@code IN_PROGRESS}.
     * This acts as a pessimistic lock.
     *
     * @param fileMasterId The ID of the {@link FileMaster} to lock.
     * @return {@code true} if the lock was successfully acquired, or {@code false} if the file was not
     * found or was not in the {@code QUEUED} state.
     */
    @Transactional
    public boolean acquireLock(final Long fileMasterId) {
        final FileMaster file = fileMasterRepository.findById(fileMasterId).orElse(null);

        if (file == null || file.getFileProcessingStatus() != FileProcessingStatus.QUEUED) {
            return false;
        }

        file.setFileProcessingStatus(FileProcessingStatus.IN_PROGRESS);
        fileMasterRepository.save(file);
        return true;
    }
}