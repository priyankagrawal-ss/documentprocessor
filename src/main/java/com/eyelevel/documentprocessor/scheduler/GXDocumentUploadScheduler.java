package com.eyelevel.documentprocessor.scheduler;

import com.eyelevel.documentprocessor.common.apiclient.gx.GXApiClient;
import com.eyelevel.documentprocessor.dto.gx.docupload.GXDocumentUploadParameters;
import com.eyelevel.documentprocessor.dto.gx.docupload.response.GXUploadDocumentResponse;
import com.eyelevel.documentprocessor.exception.apiclient.ApiException;
import com.eyelevel.documentprocessor.model.GxMaster;
import com.eyelevel.documentprocessor.model.GxStatus;
import com.eyelevel.documentprocessor.repository.GxMasterRepository;
import com.eyelevel.documentprocessor.service.S3StorageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * A scheduler that manages the uploading of processed documents to the GroundX (GX) service.
 * It respects concurrency limits and handles the upload lifecycle.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class GXDocumentUploadScheduler {

    private final GxMasterRepository gxMasterRepository;
    private final S3StorageService s3StorageService;
    private final GXApiClient gxApiClient;

    @Value("${app.gx.max-process}")
    private int maxConcurrentGxProcesses;

    /**
     * Runs on a fixed schedule to initiate document uploads to GroundX.
     * <p>
     * This method respects a concurrency limit ({@code maxConcurrentGxProcesses}) to avoid overloading
     * the external service. It fetches a batch of documents ready for upload, calls the GX API
     * to start the ingestion, and updates the database records within a single transaction.
     */
    @Scheduled(cron = "${app.scheduler.gx-doc-upload}")
    @Transactional
    public void initiateGXDocumentUpload() {
        log.info("Starting GX document upload scheduler...");

        try {
            final List<GxStatus> inProgressStatuses = List.of(GxStatus.QUEUED, GxStatus.PROCESSING);
            final long gxProcessingCount = gxMasterRepository.countByGxStatusIn(inProgressStatuses);

            if (gxProcessingCount >= maxConcurrentGxProcesses) {
                log.info("GX processing limit reached (in progress: {}, limit: {}). Skipping this run.",
                        gxProcessingCount, maxConcurrentGxProcesses);
                return;
            }

            final int availableSlots = (int) (maxConcurrentGxProcesses - gxProcessingCount);
            final List<GxMaster> documentsToUpload = gxMasterRepository.findByGxStatusOrderByCreatedAtAsc(
                    GxStatus.QUEUED_FOR_UPLOAD, PageRequest.of(0, availableSlots)
            );

            if (CollectionUtils.isEmpty(documentsToUpload)) {
                log.info("No documents are currently queued for upload to GX. Scheduler run is complete.");
                return;
            }

            log.info("Found {} documents to upload to GX. Available slots: {}", documentsToUpload.size(), availableSlots);
            final List<GxMaster> updatedMasters = new ArrayList<>();
            for (final GxMaster gxMaster : documentsToUpload) {
                processDocumentUpload(gxMaster);
                updatedMasters.add(gxMaster);
            }

            gxMasterRepository.saveAll(updatedMasters);
            log.info("Successfully initiated upload for {} documents.", updatedMasters.size());

        } catch (final Exception e) {
            log.error("An unexpected error occurred during the GX document upload scheduler run.", e);
        }
        log.info("GX document upload scheduler finished.");
    }

    /**
     * Processes a single document for upload by generating a pre-signed URL and calling the GX API.
     */
    private void processDocumentUpload(final GxMaster gxMaster) {
        try {
            final URL downloadUrl = s3StorageService.generatePresignedDownloadUrl(gxMaster.getFileLocation());
            final GXDocumentUploadParameters uploadParams = new GXDocumentUploadParameters(
                    gxMaster.getGxBucketId(),
                    gxMaster.getProcessedFileName(),
                    gxMaster.getExtension(),
                    downloadUrl.toExternalForm()
            );

            final GXUploadDocumentResponse response = gxApiClient.uploadDocument(uploadParams);
            updateMasterFromApiResponse(response, gxMaster);

        } catch (final ApiException e) {
            log.error("API error while uploading GxMaster ID {}: {}", gxMaster.getId(), e.getMessage(), e);
            gxMaster.setGxStatus(GxStatus.ERROR);
            gxMaster.setErrorMessage("API Error: " + e.getMessage());
        } catch (final Exception e) {
            log.error("Unexpected error while processing GxMaster ID {}: {}", gxMaster.getId(), e.getMessage(), e);
            gxMaster.setGxStatus(GxStatus.ERROR);
            gxMaster.setErrorMessage("Unexpected Error: " + e.getMessage());
        }
    }

    /**
     * Updates the GxMaster entity based on the response from the GX API.
     */
    private void updateMasterFromApiResponse(final GXUploadDocumentResponse response, final GxMaster gxMaster) {
        if (response == null) {
            gxMaster.setGxStatus(GxStatus.ERROR);
            gxMaster.setErrorMessage("Received a null response from GX API.");
            return;
        }

        if (response.ingest() != null && response.ingest().processId() != null) {
            gxMaster.setGxProcessId(response.ingest().processId());
            gxMaster.setGxStatus(GxStatus.convertByValue(response.ingest().status()));
            log.info("Successfully initiated GX upload for GxMaster ID {}. Process ID: {}, Status: {}",
                    gxMaster.getId(), gxMaster.getGxProcessId(), gxMaster.getGxStatus());
        } else if (StringUtils.hasText(response.message())) {
            gxMaster.setGxStatus(GxStatus.ERROR);
            gxMaster.setErrorMessage(response.message());
            log.warn("GX upload failed for GxMaster ID {} with message: {}", gxMaster.getId(), response.message());
        } else {
            gxMaster.setGxStatus(GxStatus.ERROR);
            gxMaster.setErrorMessage("Received an invalid or incomplete response from GX API.");
            log.error("Invalid response from GX for GxMaster ID {}: {}", gxMaster.getId(), response);
        }
    }
}