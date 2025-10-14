package com.eyelevel.documentprocessor.repository;

import com.eyelevel.documentprocessor.model.FileMaster;
import com.eyelevel.documentprocessor.model.FileProcessingStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface FileMasterRepository extends JpaRepository<FileMaster, Long> {
    Optional<FileMaster> findFirstByGxBucketIdAndFileHashAndFileProcessingStatus(Integer gxBucketId, String fileHash,
                                                                                 FileProcessingStatus status);

    List<FileMaster> findAllByProcessingJobId(Long jobId);

    Optional<FileMaster> findFirstByProcessingJobIdAndFileHashAndFileProcessingStatus(Long jobId, String fileHash,
                                                                                      FileProcessingStatus status);

}