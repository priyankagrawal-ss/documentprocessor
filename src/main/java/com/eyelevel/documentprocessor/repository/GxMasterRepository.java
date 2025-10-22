package com.eyelevel.documentprocessor.repository;

import com.eyelevel.documentprocessor.model.GxMaster;
import com.eyelevel.documentprocessor.model.GxStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Spring Data JPA repository for the {@link GxMaster} entity.
 */
@Repository
public interface GxMasterRepository extends JpaRepository<GxMaster, Long> {

    /**
     * Finds a {@link GxMaster} by its source {@link com.eyelevel.documentprocessor.model.FileMaster} ID.
     * The relationship is unique, so this will return at most one result.
     *
     * @param sourceFileId The ID of the source {@code FileMaster}.
     *
     * @return An {@link Optional} containing the matching {@link GxMaster}, if found.
     */
    Optional<GxMaster> findBySourceFileId(Long sourceFileId);

    /**
     * Finds a paginated list of {@link GxMaster} records with a specific status, ordered by creation date.
     *
     * @param gxStatus The status to filter by.
     * @param pageable The pagination information.
     *
     * @return A list of matching {@link GxMaster} entities.
     */
    List<GxMaster> findByGxStatusOrderByCreatedAtAsc(GxStatus gxStatus, Pageable pageable);

    /**
     * Finds a paginated list of {@link GxMaster} records that are in any of the specified statuses,
     * ordered by creation date.
     *
     * @param statuses A list of {@link GxStatus} values to search for.
     * @param pageable The pagination information.
     *
     * @return A list of matching {@link GxMaster} entities.
     */
    @Query("SELECT gm FROM GxMaster gm WHERE gm.gxStatus IN :statuses ORDER BY gm.createdAt ASC")
    List<GxMaster> findAllByStatusInOrderByCreatedAtAsc(@Param("statuses") List<GxStatus> statuses, Pageable pageable);

    /**
     * Counts the total number of {@link GxMaster} records that have one of the specified statuses.
     * This is used to monitor in-progress uploads to avoid exceeding concurrency limits.
     *
     * @param statuses A list of statuses to count (e.g., {@code QUEUED}, {@code PROCESSING}).
     *
     * @return The total count of records with the specified statuses.
     */
    long countByGxStatusIn(List<GxStatus> statuses);

    /**
     * Finds all {@link GxMaster} records associated with a given {@link com.eyelevel.documentprocessor.model.ProcessingJob} ID
     * by joining through the {@link com.eyelevel.documentprocessor.model.FileMaster} entity.
     *
     * @param jobId The ID of the parent {@code ProcessingJob}.
     *
     * @return A list of all related {@link GxMaster} entities.
     */
    @Query("SELECT gm FROM GxMaster gm JOIN gm.sourceFile fm WHERE fm.processingJob.id = :jobId")
    List<GxMaster> findAllByProcessingJobId(@Param("jobId") Long jobId);
}