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

@Repository
public interface GxMasterRepository extends JpaRepository<GxMaster, Long> {
    Optional<GxMaster> findBySourceFileId(Long sourceFileId);

    List<GxMaster> findByGxStatusOrderByCreatedAtAsc(GxStatus gxStatus, Pageable pageable);

    
    @Query("SELECT gm FROM GxMaster gm WHERE gm.gxStatus IN :statuses ORDER BY gm.createdAt ASC")
    List<GxMaster> findAllByStatusInOrderByCreatedAtAsc(
            @Param("statuses") List<GxStatus> statuses,
            Pageable pageable
    );


    /**
     * Counts all GxMaster entries where the status is 'QUEUED_FOR_UPLOAD' or 'PROCESSING'.
     * This combines your previous `gxProcessingCount()` logic into a more flexible method.
     *
     * @param statuses A list of statuses to count.
     * @return The total count of entries with the specified statuses.
     */
    long countByGxStatusIn(List<GxStatus> statuses);
}