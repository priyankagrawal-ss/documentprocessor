package com.eyelevel.documentprocessor.service.file;

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
     * This acts as a pessimistic lock by using an atomic database UPDATE statement.
     *
     * @param fileMasterId The ID of the {@link FileMaster} to lock.
     *
     * @return {@code true} if the lock was successfully acquired (record was found and updated),
     * or {@code false} if the record was not found or was not in the {@code QUEUED} state.
     */
    @Transactional
    public boolean acquireLock(final Long fileMasterId) {
        int rowsUpdated = fileMasterRepository.updateStatusIfExpected(fileMasterId, FileProcessingStatus.IN_PROGRESS,
                                                                      FileProcessingStatus.QUEUED);

        // If the query updated one row, we successfully acquired the lock.
        // If it updated zero rows, we lost the race or the record wasn't in the right state.
        return rowsUpdated > 0;
    }
}