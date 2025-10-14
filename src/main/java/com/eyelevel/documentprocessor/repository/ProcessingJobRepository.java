package com.eyelevel.documentprocessor.repository;

import com.eyelevel.documentprocessor.model.ProcessingJob;
import com.eyelevel.documentprocessor.model.ProcessingStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Spring Data JPA repository for the {@link ProcessingJob} entity.
 */
@Repository
public interface ProcessingJobRepository extends JpaRepository<ProcessingJob, Long> {

    /**
     * Finds jobs that are in a specific status and were created before a given timestamp.
     * This is used by the {@link com.eyelevel.documentprocessor.scheduler.StaleJobCleanupScheduler}
     * to identify abandoned uploads.
     *
     * @param status    The status to search for (e.g., {@code PENDING_UPLOAD}).
     * @param threshold The timestamp to compare the creation date against.
     * @return A list of matching {@link ProcessingJob} entities.
     */
    List<ProcessingJob> findByStatusAndCreatedAtBefore(ProcessingStatus status, LocalDateTime threshold);

    /**
     * Finds all jobs that are in one of the specified non-terminal statuses.
     * This is used by the {@link com.eyelevel.documentprocessor.scheduler.JobLifecycleScheduler}
     * to find active jobs that need a completion check.
     *
     * @param statuses A list of statuses to search for (e.g., {@code QUEUED}, {@code PROCESSING}).
     * @return A list of matching {@link ProcessingJob} entities.
     */
    List<ProcessingJob> findByStatusIn(List<ProcessingStatus> statuses);
}