package com.eyelevel.documentprocessor.repository;

import com.eyelevel.documentprocessor.model.ZipMaster;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Spring Data JPA repository for the {@link ZipMaster} entity.
 */
@Repository
public interface ZipMasterRepository extends JpaRepository<ZipMaster, Long> {
    // Standard CRUD operations are inherited from JpaRepository.
    // Custom queries can be added here if needed in the future.

    List<ZipMaster> findAllByProcessingJobId(Long jobId);
}