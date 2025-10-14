package com.eyelevel.documentprocessor.repository;

import com.eyelevel.documentprocessor.model.FileMaster;
import com.eyelevel.documentprocessor.model.FileProcessingStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Spring Data JPA repository for the {@link FileMaster} entity.
 */
@Repository
public interface FileMasterRepository extends JpaRepository<FileMaster, Long> {

    /**
     * Finds the first successfully completed {@link FileMaster} that matches a specific bucket and file hash.
     * This is primarily used for duplicate detection.
     *
     * @param gxBucketId The GroundX bucket ID to search within.
     * @param fileHash   The SHA-256 hash of the file content.
     * @param status     The processing status to match (typically {@code COMPLETED}).
     * @return An {@link Optional} containing the matching {@link FileMaster}, if found.
     */
    Optional<FileMaster> findFirstByGxBucketIdAndFileHashAndFileProcessingStatus(
            Integer gxBucketId, String fileHash, FileProcessingStatus status);

    /**
     * Finds all {@link FileMaster} entities associated with a specific {@link com.eyelevel.documentprocessor.model.ProcessingJob}.
     *
     * @param jobId The ID of the parent {@code ProcessingJob}.
     * @return A list of all associated {@code FileMaster} entities.
     */
    List<FileMaster> findAllByProcessingJobId(Long jobId);

    /**
     * Finds the first {@link FileMaster} that matches a specific job ID, file hash, and status.
     * This is used to re-queue a previously failed file within the same job if it is re-encountered.
     *
     * @param jobId    The ID of the parent {@code ProcessingJob}.
     * @param fileHash The SHA-256 hash of the file content.
     * @param status   The processing status to match (typically {@code FAILED}).
     * @return An {@link Optional} containing the matching {@link FileMaster}, if found.
     */
    Optional<FileMaster> findFirstByProcessingJobIdAndFileHashAndFileProcessingStatus(
            Long jobId, String fileHash, FileProcessingStatus status);
}