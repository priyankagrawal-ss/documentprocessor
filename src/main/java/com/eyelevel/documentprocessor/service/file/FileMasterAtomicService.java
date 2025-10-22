package com.eyelevel.documentprocessor.service.file;

import com.eyelevel.documentprocessor.model.FileMaster;
import com.eyelevel.documentprocessor.model.FileProcessingStatus;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class FileMasterAtomicService {

    private final FileMasterRepository fileMasterRepository;

    /**
     * Finds the definitive "winning" record for a given hash in a new, read-only transaction.
     * This can be safely called at any time.
     *
     * @param gxBucketId The bucket ID.
     * @param fileHash The file hash.
     * @return An Optional containing the winning FileMaster if it exists.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Optional<FileMaster> findWinner(Integer gxBucketId, String fileHash) {
        return fileMasterRepository.findFirstByGxBucketIdAndFileHashAndFileProcessingStatusNotInOrderByIdAsc(
                gxBucketId,
                fileHash,
                List.of(FileProcessingStatus.FAILED, FileProcessingStatus.IGNORED)
                                                                                                            );
    }

    /**
     * Attempts to create a new FileMaster record in its own new transaction.
     * This method is designed to be called only when the caller believes no duplicate exists.
     * It will throw a DataIntegrityViolationException if a race condition occurs, which the caller must handle.
     *
     * @param potentialNewFile The new FileMaster to save.
     * @return The saved FileMaster with its generated ID.
     * @throws DataIntegrityViolationException if a duplicate record already exists.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public FileMaster attemptToCreate(FileMaster potentialNewFile) throws DataIntegrityViolationException {
        return fileMasterRepository.saveAndFlush(potentialNewFile);
    }
}