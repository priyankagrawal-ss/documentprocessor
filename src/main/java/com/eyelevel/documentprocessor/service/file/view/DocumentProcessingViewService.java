package com.eyelevel.documentprocessor.service.file.view;

import com.eyelevel.documentprocessor.dto.metric.StatusMetric;
import com.eyelevel.documentprocessor.dto.metric.response.StatusMetricItem;
import com.eyelevel.documentprocessor.repository.DocumentProcessingViewRepository;
import com.eyelevel.documentprocessor.view.DocumentProcessingView;
import com.smartsensesolutions.commons.dao.base.BaseRepository;
import com.smartsensesolutions.commons.dao.base.BaseService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service class for querying the {@link DocumentProcessingView}.
 * <p>
 * This service provides methods to retrieve paginated and filtered lists of documents
 * based on their processing status from a read-only view.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class DocumentProcessingViewService extends BaseService<DocumentProcessingView, Long> {

    private final DocumentProcessingViewRepository documentProcessingViewRepository;

    /**
     * Retrieves status metrics for a given list of bucket IDs.
     * The result is structured as a map where each key is a bucket ID and the value is a list of its status counts.
     *
     * @param gxBucketIds The list of bucket IDs to query.
     *
     * @return A map of bucket IDs to their corresponding list of status metrics.
     */
    public Map<Integer, List<StatusMetricItem>> getMetricsForBuckets(List<Integer> gxBucketIds) {
        if (gxBucketIds == null || gxBucketIds.isEmpty()) {
            return Map.of();
        }

        log.debug("Fetching status metrics for gxBucketIds: {}", gxBucketIds);
        // The repository now returns a list of our clean DTO
        List<StatusMetric> flatResults = documentProcessingViewRepository.findStatusMetricsByBucketIds(gxBucketIds);

        // Transform the flat list into the desired nested map structure
        return flatResults.stream().collect(Collectors.groupingBy(StatusMetric::getGxBucketId, Collectors.mapping(
                metric -> new StatusMetricItem(metric.getDisplayStatus(), metric.getStatusCount()),
                Collectors.toList())));
    }

    /**
     * Provides the underlying repository for the base service.
     *
     * @return The {@link DocumentProcessingViewRepository} instance.
     */
    @Override
    protected BaseRepository<DocumentProcessingView, Long> getRepository() {
        return documentProcessingViewRepository;
    }
}