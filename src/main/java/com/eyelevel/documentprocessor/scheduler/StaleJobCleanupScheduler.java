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

import java.time.LocalDateTime;
import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class StaleJobCleanupScheduler {

    private final ProcessingJobRepository processingJobRepository;

    @Value("${app.scheduler.stale-job-cleanup-hours}")
    private long staleThresholdHours;

    /**
     * Periodically runs to find jobs that have been in the PENDING_UPLOAD state for too long.
     * This cleans up abandoned uploads where the client never triggered the processing.
     */
    @Scheduled(cron = "${app.scheduler.stale-job}") // Run at the top of every hour
    @Transactional
    public void markStaleJobsAsFailed() {
        LocalDateTime threshold = LocalDateTime.now().minusHours(staleThresholdHours);
        StaleJobCleanupScheduler.log.info("Running stale job cleanup. Finding jobs in PENDING_UPLOAD created before {}.", threshold);

        List<ProcessingJob> staleJobs = processingJobRepository.findByStatusAndCreatedAtBefore(
                ProcessingStatus.PENDING_UPLOAD, threshold
        );

        if (staleJobs.isEmpty()) {
            StaleJobCleanupScheduler.log.info("No stale jobs found.");
            return;
        }

        for (ProcessingJob job : staleJobs) {
            StaleJobCleanupScheduler.log.warn("Job ID {} is stale. Marking as FAILED.", job.getId());
            job.setStatus(ProcessingStatus.FAILED);
            job.setErrorMessage("Upload was not completed and triggered by the client within the " + staleThresholdHours + "-hour time limit.");
            processingJobRepository.save(job);
        }
        StaleJobCleanupScheduler.log.info("Finished stale job cleanup. Marked {} jobs as FAILED.", staleJobs.size());
    }
}