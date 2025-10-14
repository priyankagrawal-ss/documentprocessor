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
import org.springframework.util.StringUtils;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Schedules and manages the uploading of documents to the GroundX (GX) service.
 * <p>
 * This scheduler periodically checks for documents that are queued for upload,
 * respects the configured concurrent processing limits, and handles the entire
 * upload lifecycle including status updates and error handling.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class GXDocumentUploadScheduler {

    private final GxMasterRepository gxMasterRepository;
    private final S3StorageService s3StorageService;
    private final GXApiClient gxApiClient;

    @Value("${app.gx.max-process}")
    private int maxGXProcess;

    /**
     * Periodically initiates the document upload process to GroundX.
     * <p>
     * This method runs based on a cron expression and performs the following steps:
     * 1. Checks the number of documents currently being processed by GX.
     * 2. If the processing limit is reached, it skips the current run.
     * 3. Fetches a batch of documents with the status {@link GxStatus#QUEUED_FOR_UPLOAD}.
     * 4. For each document, it generates a pre-signed S3 URL and calls the GX API to start the upload.
     * 5. Updates the document's status in the database based on the API response.
     * 6. All database updates within a single run are performed in a single transaction.
     */
    @Scheduled(cron = "${app.scheduler.gx-doc-upload}")
    @Transactional
    public void initiateGXDocumentUpload() {
        log.info("Starting GX document upload scheduler run.");

        try {
            List<GxStatus> processingStatus = List.of(GxStatus.QUEUED, GxStatus.PROCESSING);
            final long gxProcessingCount = gxMasterRepository.countByGxStatusIn(processingStatus);

            if (gxProcessingCount >= maxGXProcess) {
                log.info(
                        "GX processing limit reached. {} tasks are already in progress. Skipping this run.",
                        gxProcessingCount
                );
                return;
            }

            final int availableBandwidth = (int) (maxGXProcess - gxProcessingCount);
            final List<GxMaster> documentsToUpload = gxMasterRepository.findByGxStatusOrderByCreatedAtAsc(
                    GxStatus.QUEUED_FOR_UPLOAD,
                    PageRequest.of(0, availableBandwidth)
            );

            if (documentsToUpload.isEmpty()) {
                log.info("No documents are currently queued for upload to GX.");
                return;
            }

            log.info("Found {} documents to upload to GX. Available bandwidth: {}", documentsToUpload.size(), availableBandwidth);

            final List<GxMaster> updatedMasters = new ArrayList<>();
            for (final GxMaster gxMaster : documentsToUpload) {
                processDocument(gxMaster);
                updatedMasters.add(gxMaster);
            }

            gxMasterRepository.saveAll(updatedMasters);
            log.info("Successfully processed {} documents in this scheduler run.", updatedMasters.size());

        } catch (final Exception e) {
            log.error("An unexpected error occurred during the GX document upload scheduler run.", e);
        }
    }

    /**
     * Processes a single document for upload.
     *
     * @param gxMaster The GxMaster entity representing the document.
     */
    private void processDocument(final GxMaster gxMaster) {
        try {
            final URL downloadUrl = s3StorageService.generatePresignedDownloadUrl(gxMaster.getFileLocation());

            final GXDocumentUploadParameters uploadParameters = new GXDocumentUploadParameters(
                    gxMaster.getGxBucketId(),
                    gxMaster.getProcessedFileName(),
                    gxMaster.getExtension(),
                    downloadUrl.toExternalForm()
            );

            final GXUploadDocumentResponse response = gxApiClient.uploadDocument(uploadParameters);
            handleApiResponse(response, gxMaster);

        } catch (final ApiException e) {
            log.error("API error while uploading document ID {}: {}", gxMaster.getId(), e.getMessage(), e);
            gxMaster.setGxStatus(GxStatus.ERROR);
            gxMaster.setErrorMessage("API Error: " + e.getMessage());
        } catch (final Exception e) {
            log.error("An unexpected error occurred while processing document ID {}: {}", gxMaster.getId(), e.getMessage(), e);
            gxMaster.setGxStatus(GxStatus.ERROR);
            gxMaster.setErrorMessage("Unexpected Error: " + e.getMessage());
        }
    }

    /**
     * Updates the GxMaster entity based on the response from the GX API.
     *
     * @param response The response from the document upload API call.
     * @param gxMaster The GxMaster entity to update.
     */
    private void handleApiResponse(final GXUploadDocumentResponse response, final GxMaster gxMaster) {
        if (response == null) {
            gxMaster.setGxStatus(GxStatus.ERROR);
            gxMaster.setErrorMessage("Received a null response from GX API.");
            return;
        }

        // Check for error messages returned by the API in the message field
        if (StringUtils.hasText(response.message()) && Objects.isNull(response.ingest())) {
            gxMaster.setGxStatus(GxStatus.ERROR);
            gxMaster.setErrorMessage(response.message());
            log.warn("GX upload failed for document ID {} with message: {}", gxMaster.getId(), response.message());
        } else if (response.ingest() != null && response.ingest().processId() != null) {
            gxMaster.setGxProcessId(response.ingest().processId());
            gxMaster.setGxStatus(GxStatus.convertByValue(response.ingest().status()));
            log.info(
                    "Successfully initiated GX upload for document ID {}. Process ID: {}, Status: {}",
                    gxMaster.getId(),
                    gxMaster.getGxProcessId(),
                    gxMaster.getGxStatus()
            );
        } else {
            // Handle unexpected response structure
            gxMaster.setGxStatus(GxStatus.ERROR);
            gxMaster.setErrorMessage("Received an invalid or incomplete response from GX API.");
            log.error("Invalid response from GX for document ID {}: {}", gxMaster.getId(), response);
        }
    }
}