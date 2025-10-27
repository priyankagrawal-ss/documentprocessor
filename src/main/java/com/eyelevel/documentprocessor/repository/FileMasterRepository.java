package com.eyelevel.documentprocessor.repository;

import com.eyelevel.documentprocessor.model.FileMaster;
import com.eyelevel.documentprocessor.model.FileProcessingStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * Spring Data JPA repository for the {@link FileMaster} entity.
 * JPQL queries are defined in META-INF/file-master-orm.xml.
 */
@Repository
public interface FileMasterRepository extends JpaRepository<FileMaster, Long> {

    List<FileMaster> findAllByProcessingJobId(Long jobId);

    @Modifying
    @Query(name = "FileMaster.updateStatusForJobIds")
    int updateStatusForJobIds(@Param("jobId") List<Long> jobId, @Param("newStatus") FileProcessingStatus newStatus,
                              @Param("statusesToUpdate") List<FileProcessingStatus> statusesToUpdate);

    @Modifying
    @Query(name = "FileMaster.updateStatusIfExpected")
    int updateStatusIfExpected(@Param("id") Long id, @Param("newStatus") FileProcessingStatus newStatus,
                               @Param("expectedStatus") FileProcessingStatus expectedStatus);

    Optional<FileMaster> findByProcessingJobId(Long jobId);

    @Transactional(readOnly = true)
    Optional<FileMaster> findFirstByGxBucketIdAndFileHashAndFileProcessingStatusNotInOrderByIdAsc(Integer gxBucketId,
                                                                                                  String fileHash,
                                                                                                  List<FileProcessingStatus> statuses);

    @Transactional(readOnly = true)
    Optional<FileMaster> findFirstByGxBucketIdAndFileHashAndIdNotAndFileProcessingStatusNotInOrderByIdAsc(
            Integer gxBucketId, String fileHash, Long idToExclude, List<FileProcessingStatus> statuses);

    @Query(name = "FileMaster.findByIdWithJob")
    Optional<FileMaster> findByIdWithJob(@Param("id") Long id);

    @Query(name = "FileMaster.findFileLocationById")
    Optional<String> findFileLocationById(@Param("id") Long id);
}