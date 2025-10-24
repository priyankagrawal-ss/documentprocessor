package com.eyelevel.documentprocessor.repository;

import com.eyelevel.documentprocessor.model.ZipMaster;
import com.eyelevel.documentprocessor.model.ZipProcessingStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Spring Data JPA repository for the {@link ZipMaster} entity.
 * JPQL queries are defined in META-INF/zip-master-orm.xml.
 */
@Repository
public interface ZipMasterRepository extends JpaRepository<ZipMaster, Long> {
    List<ZipMaster> findAllByProcessingJobId(Long jobId);

    Optional<ZipMaster> findByProcessingJobId(Long jobId);

    @Modifying
    @Query(name = "ZipMaster.updateStatusForJobIds")
        // Ensure this named query is defined
    int updateStatusForJobIds(@Param("jobIds") List<Long> jobIds,
                              @Param("newStatus") ZipProcessingStatus newStatus,
                              @Param("statusesToUpdate") List<ZipProcessingStatus> statusesToUpdate);

    @Query(name = "ZipMaster.findByIdWithJob")
    Optional<ZipMaster> findByIdWithJob(@Param("id") Long id);
}