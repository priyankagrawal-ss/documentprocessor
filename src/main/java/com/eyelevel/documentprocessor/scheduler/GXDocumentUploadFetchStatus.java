package com.eyelevel.documentprocessor.scheduler;

import com.eyelevel.documentprocessor.common.apiclient.gx.GXApiClient;
import com.eyelevel.documentprocessor.dto.gx.uploadstatus.response.IngestResponse;
import com.eyelevel.documentprocessor.model.GxMaster;
import com.eyelevel.documentprocessor.model.GxStatus;
import com.eyelevel.documentprocessor.repository.GxMasterRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A scheduler responsible for periodically fetching the ingestion status of documents
 * from the external GroundX (GX) service.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class GXDocumentUploadFetchStatus {

    private final GxMasterRepository gxMasterRepository;
    private final GXApiClient gxApiClient;

    /**
     * Runs on a fixed schedule to query the status of all documents currently being processed by GX.
     * <p>
     * This method fetches all {@link GxMaster} records in {@code PROCESSING} or {@code QUEUED} states,
     * calls the GX API for each, and updates their status in the database in a single batch transaction.
     * Failures are isolated to prevent one failed API call from halting the entire process.
     */
    @Scheduled(cron = "${app.scheduler.fetch-doc-upload-status}")
    @Transactional
    public void fetchDocumentUploadStatus() {
        log.info("Starting GX document status fetch scheduler...");

        final List<GxStatus> activeStatuses = List.of(GxStatus.PROCESSING, GxStatus.QUEUED);
        List<GxMaster> activeProcesses =
                gxMasterRepository.findAllByGxStatusInOrderByCreatedAtAsc(activeStatuses, Pageable.unpaged())
                        .getContent();
        
        if (CollectionUtils.isEmpty(activeProcesses)) {
            log.info("No active GX processes found. Scheduler run is complete.");
            return;
        }

        log.info("Found {} active GX processes to check for status updates.", activeProcesses.size());
        final List<GxMaster> mastersToUpdate = new ArrayList<>();

        for (final GxMaster gxMaster : activeProcesses) {
            try {
                log.debug("Fetching status for GxMaster ID: {}, Process ID: {}", gxMaster.getId(),
                        gxMaster.getGxProcessId());
                final IngestResponse ingestResponse = gxApiClient.fetchUploadDocumentStatus(gxMaster.getGxProcessId());

                extractDocumentDetails(ingestResponse).ifPresentOrElse(documentDetails -> {
                    updateMasterFromDocumentDetails(gxMaster, documentDetails);
                    mastersToUpdate.add(gxMaster);
                }, () -> log.warn("Could not find document details in GX response for process ID: {}",
                        gxMaster.getGxProcessId()));

            } catch (final Exception e) {
                log.error("Failed to fetch or process status for GxMaster ID: {}. Marking as ERROR.", gxMaster.getId(),
                        e);
                gxMaster.setGxStatus(GxStatus.ERROR);
                gxMaster.setErrorMessage("Failed to retrieve status from GX: " + e.getMessage());
                mastersToUpdate.add(gxMaster);
            }
        }

        if (!mastersToUpdate.isEmpty()) {
            gxMasterRepository.saveAll(mastersToUpdate);
            log.info("Successfully updated status for {} GxMaster records.", mastersToUpdate.size());
        }

        log.info("GX document status fetch scheduler finished successfully.");
    }

    /**
     * Safely extracts the first available document details from the ingestion response.
     * It checks categories in order of finality: complete, errors, cancelled, and finally processing.
     *
     * @param ingestResponse The response from the GX API.
     * @return An {@link Optional} containing the document details if found.
     */
    private Optional<IngestResponse.Document> extractDocumentDetails(final IngestResponse ingestResponse) {
        if (ingestResponse == null || ingestResponse.ingest() == null || ingestResponse.ingest().progress() == null) {
            return Optional.empty();
        }
        final IngestResponse.Progress progress = ingestResponse.ingest().progress();
        return findFirstDocumentInCategory(progress.complete()).or(() -> findFirstDocumentInCategory(progress.errors()))
                .or(() -> findFirstDocumentInCategory(
                        progress.cancelled()))
                .or(() -> findFirstDocumentInCategory(
                        progress.processing()));
    }

    /**
     * A helper method to safely get the first document from a progress category.
     */
    private Optional<IngestResponse.Document> findFirstDocumentInCategory(
            final IngestResponse.ProgressCategory category) {
        return (category != null && !CollectionUtils.isEmpty(category.documents())) ? Optional.of(
                category.documents().getFirst()) : Optional.empty();
    }

    /**
     * Updates a GxMaster entity based on the details from the API response.
     */
    private void updateMasterFromDocumentDetails(final GxMaster gxMaster, final IngestResponse.Document document) {
        final GxStatus newStatus = GxStatus.convertByValue(document.status());
        gxMaster.setGxStatus(newStatus);
        if (StringUtils.hasText(document.statusMessage())) {
            gxMaster.setErrorMessage(document.statusMessage());
        }
        log.info("Updating GxMaster ID: {}. New status: {}, Message: '{}'", gxMaster.getId(), newStatus,
                gxMaster.getErrorMessage());
    }
}