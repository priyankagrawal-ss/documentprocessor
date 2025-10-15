package com.eyelevel.documentprocessor.repository;

import com.eyelevel.documentprocessor.model.FileMaster;
import com.eyelevel.documentprocessor.model.FileProcessingStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
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
     * Finds the first completed FileMaster that matches a given bucket and ORIGINAL content hash.
     * This is the definitive method for duplicate detection, as it's immune to changes
     * from file optimization.
     *
     * @param gxBucketId          The bucket ID.
     * @param originalContentHash The immutable hash of the original file content.
     * @param status              The status to check against (typically COMPLETED).
     * @return An Optional containing the duplicate FileMaster, if found.
     */
    Optional<FileMaster> findFirstByGxBucketIdAndOriginalContentHashAndFileProcessingStatus(
            Integer gxBucketId, String originalContentHash, FileProcessingStatus status
    );

    /**
     * Finds a completed duplicate by checking a hash against both the original and final
     * content hashes of existing records. This is the definitive duplicate check.
     *
     * @param gxBucketId The bucket ID to search within.
     * @param hash       The hash of the new file to check.
     * @param status     The status to filter by (e.g., COMPLETED).
     * @return A list of matching duplicates, ordered by ID.
     */
    @Query("SELECT fm FROM FileMaster fm WHERE fm.gxBucketId = :gxBucketId AND fm.fileProcessingStatus = :status " +
           "AND (fm.originalContentHash = :hash OR fm.fileHash = :hash) " +
           "ORDER BY fm.id ASC")
    List<FileMaster> findCompletedDuplicateByHash(
            @Param("gxBucketId") Integer gxBucketId,
            @Param("hash") String hash,
            @Param("status") FileProcessingStatus status
    );
}