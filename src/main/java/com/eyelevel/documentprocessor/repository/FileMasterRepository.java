package com.eyelevel.documentprocessor.repository;

import com.eyelevel.documentprocessor.model.FileMaster;
import com.eyelevel.documentprocessor.model.FileProcessingStatus;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * Spring Data JPA repository for the {@link FileMaster} entity.
 */
@Repository
public interface FileMasterRepository extends JpaRepository<FileMaster, Long> {

    Optional<FileMaster> findFirstByGxBucketIdAndFileHashAndFileProcessingStatusNotIn(Integer gxBucketId,
                                                                                      String fileHash,
                                                                                      List<FileProcessingStatus> statuses);

    List<FileMaster> findAllByGxBucketIdAndFileHashAndFileProcessingStatusNotIn(Integer gxBucketId, String fileHash,
                                                                                List<FileProcessingStatus> statuses);

    /**
     * Finds all {@link FileMaster} entities associated with a specific {@link com.eyelevel.documentprocessor.model.ProcessingJob}.
     *
     * @param jobId The ID of the parent {@code ProcessingJob}.
     *
     * @return A list of all associated {@code FileMaster} entities.
     */
    List<FileMaster> findAllByProcessingJobId(Long jobId);

    /**
     * Atomically updates the status of a FileMaster record only if its current status
     * matches the expected status.
     *
     * @param id             The ID of the FileMaster to update.
     * @param newStatus      The new status to set.
     * @param expectedStatus The status the record must currently have for the update to occur.
     *
     * @return The number of rows affected (1 if the lock was acquired, 0 otherwise).
     */
    @Modifying
    @Query("UPDATE FileMaster fm SET fm.fileProcessingStatus = :newStatus WHERE fm.id = :id AND fm.fileProcessingStatus = :expectedStatus")
    int updateStatusIfExpected(@Param("id") Long id, @Param("newStatus") FileProcessingStatus newStatus,
                               @Param("expectedStatus") FileProcessingStatus expectedStatus);

    Optional<FileMaster> findByProcessingJobId(Long jobId);

    @Transactional(readOnly = true)
    Optional<FileMaster> findFirstByGxBucketIdAndFileHashAndFileProcessingStatusNotInOrderByIdAsc(Integer gxBucketId,
                                                                                                  String fileHash,
                                                                                                  List<FileProcessingStatus> statuses);
    @Transactional(readOnly = true)
    Optional<FileMaster> findFirstByGxBucketIdAndFileHashAndIdNotAndFileProcessingStatusNotInOrderByIdAsc(
            Integer gxBucketId,
            String fileHash,
            Long idToExclude,
            List<FileProcessingStatus> statuses
                                                                                                         );
}