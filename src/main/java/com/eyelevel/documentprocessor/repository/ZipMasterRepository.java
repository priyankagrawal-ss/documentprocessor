package com.eyelevel.documentprocessor.repository;

import com.eyelevel.documentprocessor.model.ZipMaster;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Spring Data JPA repository for the {@link ZipMaster} entity.
 */
@Repository
public interface ZipMasterRepository extends JpaRepository<ZipMaster, Long> {
    List<ZipMaster> findAllByProcessingJobId(Long jobId);

    Optional<ZipMaster> findByProcessingJobId(Long jobId);
}