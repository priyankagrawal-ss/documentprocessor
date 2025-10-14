package com.eyelevel.documentprocessor.repository;

import com.eyelevel.documentprocessor.model.ProcessingJob;
import com.eyelevel.documentprocessor.model.ProcessingStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface ProcessingJobRepository extends JpaRepository<ProcessingJob, Long> {

    // Used by the status sync scheduler
    List<ProcessingJob> findByStatusIn(List<ProcessingStatus> statuses);

    List<ProcessingJob> findByStatusAndCreatedAtBefore(ProcessingStatus status, LocalDateTime threshold);

}