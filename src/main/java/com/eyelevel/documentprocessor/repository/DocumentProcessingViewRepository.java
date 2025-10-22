package com.eyelevel.documentprocessor.repository;

import com.eyelevel.documentprocessor.dto.metric.StatusMetric;
import com.eyelevel.documentprocessor.view.DocumentProcessingView;
import com.smartsensesolutions.commons.dao.base.BaseRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Spring Data repository for the {@link DocumentProcessingView} entity.
 * <p>
 * This repository provides data access methods for the read-only document processing view.
 */
@Repository
public interface DocumentProcessingViewRepository extends BaseRepository<DocumentProcessingView, Long> {

    /**
     * Finds status metrics for a given list of bucket IDs.
     * This method is now automatically bound to the named native query
     * "DocumentProcessingView.findStatusMetricsByBucketIds" defined in metrics-queries.xml.
     * The results are mapped to the StatusMetric DTO via the "StatusMetricMapping".
     */
    @Query(name = "DocumentProcessingView.findStatusMetricsByBucketIds.native", nativeQuery = true)
    List<StatusMetric> findStatusMetricsByBucketIds(@Param("gxBucketIds") List<Integer> gxBucketIds);
}