package com.eyelevel.documentprocessor.scheduler;

import com.eyelevel.documentprocessor.model.ProcessingJob;
import com.eyelevel.documentprocessor.model.ProcessingStatus;
import com.eyelevel.documentprocessor.repository.ProcessingJobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.List;

/**
 * A scheduler that cleans up stale jobs that were initiated but never processed.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StaleJobCleanupScheduler {

    private final ProcessingJobRepository processingJobRepository;

    @Value("${app.scheduler.stale-job-cleanup-hours}")
    private long staleThresholdHours;

    /**
     * Periodically finds and marks jobs as FAILED if they have remained in the
     * {@code PENDING_UPLOAD} state for too long. This handles cases where a client
     * generates an upload URL but never uploads the file and triggers processing.
     */
    @Scheduled(cron = "${app.scheduler.stale-job}")
    @Transactional
    public void markStaleJobsAsFailed() {
        final LocalDateTime threshold = LocalDateTime.now().minusHours(staleThresholdHours);
        log.info("Running stale job cleanup. Finding jobs in PENDING_UPLOAD created before {}.", threshold);

        final List<ProcessingJob> staleJobs = processingJobRepository.findByStatusAndCreatedAtBefore(
                ProcessingStatus.PENDING_UPLOAD, threshold
        );

        if (CollectionUtils.isEmpty(staleJobs)) {
            log.info("No stale jobs found.");
            return;
        }

        log.warn("Found {} stale jobs to mark as FAILED.", staleJobs.size());
        for (final ProcessingJob job : staleJobs) {
            job.setStatus(ProcessingStatus.FAILED);
            job.setErrorMessage(String.format(
                    "Upload was not completed by the client within the %d-hour time limit.", staleThresholdHours));
            processingJobRepository.save(job);
        }
        log.info("Finished stale job cleanup. Marked {} jobs as FAILED.", staleJobs.size());
    }
}