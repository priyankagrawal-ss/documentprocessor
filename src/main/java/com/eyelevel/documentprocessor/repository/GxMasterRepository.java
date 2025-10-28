package com.eyelevel.documentprocessor.repository;

import com.eyelevel.documentprocessor.model.GxMaster;
import com.eyelevel.documentprocessor.model.GxStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Spring Data JPA repository for the {@link GxMaster} entity.
 * JPQL queries are defined in META-INF/gx-master-orm.xml.
 */
@Repository
public interface GxMasterRepository extends JpaRepository<GxMaster, Long> {

    Optional<GxMaster> findBySourceFileId(Long sourceFileId);


    List<GxMaster> findByGxStatusOrderByCreatedAtAsc(GxStatus gxStatus, Pageable pageable);

    @Query(name = "GxMaster.findAllByProcessingJobId")
    List<GxMaster> findAllByProcessingJobId(@Param("jobId") Long jobId);

    long countByGxStatusIn(List<GxStatus> statuses);

    @Modifying
    @Query(name = "GxMaster.updateStatusForJobIds")
    int updateStatusForJobIds(@Param("jobId") List<Long> jobId, @Param("newStatus") GxStatus newStatus,
                              @Param("statusesToUpdate") List<GxStatus> statusesToUpdate);

    Page<GxMaster> findAllByGxStatusInOrderByCreatedAtAsc(
            List<GxStatus> statuses,
            Pageable pageable
    );

    @Query(name = "GxMaster.findFileLocationById")
    Optional<String> findFileLocationById(@Param("id") Long id);

}
