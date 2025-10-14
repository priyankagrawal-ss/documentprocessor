package com.eyelevel.documentprocessor.scheduler;

import com.eyelevel.documentprocessor.common.apiclient.gx.GXApiClient;
import com.eyelevel.documentprocessor.dto.gx.uploadstatus.response.IngestResponse;
import com.eyelevel.documentprocessor.model.GxMaster;
import com.eyelevel.documentprocessor.model.GxStatus;
import com.eyelevel.documentprocessor.repository.GxMasterRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Schedules and manages fetching the status of in-progress document uploads from the GroundX (GX) service.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class GXDocumentUploadFetchStatus {

    private final GxMasterRepository gxMasterRepository;
    private final GXApiClient gxApiClient;

    /**
     * Periodically fetches the status of documents that are currently being processed by GroundX.
     * <p>
     * This method runs on a schedule to:
     * 1. Find all documents with a status of {@link GxStatus#PROCESSING} or {@link GxStatus#QUEUED}.
     * 2. For each document, call the GX API to get the latest ingestion status.
     * 3. Isolate failures so that one failed API call does not stop the entire batch.
     * 4. Update the document's status and error message in the database in a single batch operation.
     */
    @Scheduled(cron = "${app.scheduler.fetch-doc-upload-status}")
    @Transactional
    public void fetchDocumentUploadStatus() {
        log.info("Starting GX document status fetch scheduler run.");

        final List<GxStatus> activeStatuses = List.of(GxStatus.PROCESSING, GxStatus.QUEUED);
        final List<GxMaster> activeProcesses = gxMasterRepository.findAllByStatusInOrderByCreatedAtAsc(
                activeStatuses,
                PageRequest.of(0, Integer.MAX_VALUE));

        if (CollectionUtils.isEmpty(activeProcesses)) {
            log.info("No active GX processes found. Skipping this run.");
            return;
        }

        log.info("Found {} active GX processes to check for status updates.", activeProcesses.size());

        final List<GxMaster> mastersToUpdate = new ArrayList<>();
        for (final GxMaster gxMaster : activeProcesses) {
            try {
                log.debug("Fetching status for document ID: {}, Process ID: {}", gxMaster.getId(), gxMaster.getGxProcessId());

                final IngestResponse ingestResponse = gxApiClient.fetchUploadDocumentStatus(gxMaster.getGxProcessId());

                final Optional<IngestResponse.Document> documentDetailsOpt = extractDocumentDetails(ingestResponse);

                if (documentDetailsOpt.isPresent()) {
                    updateMasterFromDocumentDetails(gxMaster, documentDetailsOpt.get());
                    mastersToUpdate.add(gxMaster);
                } else {
                    log.warn("Could not find document details in any progress category for process ID: {}", gxMaster.getGxProcessId());
                }

            } catch (final Exception e) {
                log.error("Failed to fetch or process status for document ID: {}", gxMaster.getId(), e);
                gxMaster.setGxStatus(GxStatus.ERROR);
                gxMaster.setErrorMessage("Failed to retrieve status from GX: " + e.getMessage());
                mastersToUpdate.add(gxMaster);
            }
        }

        if (!mastersToUpdate.isEmpty()) {
            gxMasterRepository.saveAll(mastersToUpdate);
            log.info("Successfully updated status for {} documents.", mastersToUpdate.size());
        }

        log.info("GX document status fetch scheduler run finished.");
    }

    /**
     * Safely extracts the first available document details from the ingestion response.
     * It checks categories in order of finality: complete, errors, cancelled, and finally processing.
     *
     * @param ingestResponse The response from the GX API.
     * @return An Optional containing the {@link IngestResponse.Document} if found, otherwise an empty Optional.
     */
    private Optional<IngestResponse.Document> extractDocumentDetails(final IngestResponse ingestResponse) {
        if (ingestResponse == null || ingestResponse.ingest() == null || ingestResponse.ingest().progress() == null) {
            return Optional.empty();
        }

        final IngestResponse.Progress progress = ingestResponse.ingest().progress();

        return findFirstDocumentInCategory(progress.complete())
                .or(() -> findFirstDocumentInCategory(progress.errors()))
                .or(() -> findFirstDocumentInCategory(progress.cancelled()))
                .or(() -> findFirstDocumentInCategory(progress.processing()));
    }

    /**
     * Updates a GxMaster entity based on the details from the API response.
     *
     * @param gxMaster The entity to update.
     * @param document The document details from the API.
     */
    private void updateMasterFromDocumentDetails(final GxMaster gxMaster, final IngestResponse.Document document) {
        final GxStatus newStatus = GxStatus.convertByValue(document.status());
        gxMaster.setGxStatus(newStatus);

        final String statusMessage = document.statusMessage();
        if (StringUtils.hasText(statusMessage)) {
            gxMaster.setErrorMessage(statusMessage);
        }

        log.info("Updating document ID: {}. New status: {}. Message: '{}'", gxMaster.getId(), newStatus, statusMessage);
    }

    /**
     * A helper method to safely get the first document from a progress category.
     *
     * @param progressCategory The category to check.
     * @return An Optional containing the document if it exists, otherwise empty.
     */
    private Optional<IngestResponse.Document> findFirstDocumentInCategory(final IngestResponse.ProgressCategory progressCategory) {
        if (progressCategory != null && !CollectionUtils.isEmpty(progressCategory.documents())) {
            return Optional.of(progressCategory.documents().getFirst());
        }
        return Optional.empty();
    }
}