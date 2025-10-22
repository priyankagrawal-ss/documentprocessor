package com.eyelevel.documentprocessor.controller;

import com.eyelevel.documentprocessor.dto.PresignedUploadResponse;
import com.eyelevel.documentprocessor.dto.metric.response.StatusMetricItem;
import com.eyelevel.documentprocessor.dto.metric.request.MetricsRequest;
import com.eyelevel.documentprocessor.service.file.view.DocumentProcessingViewService;
import com.eyelevel.documentprocessor.service.job.JobOrchestrationService;
import com.eyelevel.documentprocessor.view.DocumentProcessingView;
import com.smartsensesolutions.commons.dao.filter.FilterRequest;
import com.smartsensesolutions.commons.dao.operator.Operator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST controller for managing the document ingestion and processing workflow.
 * <p>
 * This controller exposes endpoints to orchestrate a two-stage document upload process:
 * 1. Generate a pre-signed URL for direct client-to-S3 uploads.
 * 2. Trigger the backend processing pipeline after the upload is complete.
 * <p>
 * It also provides an endpoint to list and filter processed documents.
 */
@Slf4j
@RestController
@RequestMapping("/documents")
@RequiredArgsConstructor
public class DocumentProcessingController {

    private final JobOrchestrationService jobOrchestrationService;
    private final DocumentProcessingViewService documentProcessingViewService;

    /**
     * Stage 1: Creates a new document processing job and returns a pre-signed S3 URL for uploading a file.
     * This allows the client to upload a file directly to the object store securely without
     * passing through the application server.
     *
     * @param fileName      The original name of the file to be uploaded.
     * @param gxBucketId    Optional identifier for grouping documents (e.g., a client or project ID).
     * @param skipGxProcess Optional flag to bypass a specific step in the processing pipeline.
     *
     * @return A {@link ResponseEntity} containing the {@link PresignedUploadResponse} with the job ID and the upload URL.
     */
    @PostMapping("/v1/upload-url")
    public ResponseEntity<PresignedUploadResponse> createUploadUrl(@RequestParam("fileName") final String fileName,
                                                                   @RequestParam(value = "gxBucketId", required = false)
                                                                   final Integer gxBucketId,
                                                                   @RequestParam(value = "skipGxProcess",
                                                                                 defaultValue = "false")
                                                                   final boolean skipGxProcess) {
        log.info("Received request to generate upload URL for fileName: '{}', gxBucketId: {}", fileName,
                 gxBucketId == null ? "BULK" : gxBucketId);

        final PresignedUploadResponse response = jobOrchestrationService.createJobAndPresignedUrl(fileName, gxBucketId,
                                                                                                  skipGxProcess);

        log.info("Successfully generated pre-signed URL for jobId: {}", response.getJobId());
        return ResponseEntity.ok(response);
    }

    /**
     * Stage 2: Triggers the backend processing for a file that has been successfully uploaded.
     * The client should call this endpoint after receiving a successful (e.g., HTTP 200) response
     * from S3 for the direct upload.
     *
     * @param jobId The unique identifier of the job, returned by the {@code /v1/upload-url} endpoint.
     *
     * @return An {@link ResponseEntity} with HTTP status 202 (Accepted) to indicate that the processing request has been queued.
     */
    @PostMapping("/v1/process")
    public ResponseEntity<Void> startProcessing(@RequestParam("jobId") final Long jobId) {
        log.info("Received request to trigger processing for jobId: {}", jobId);

        jobOrchestrationService.triggerProcessing(jobId);

        log.info("Processing has been accepted and queued for jobId: {}", jobId);
        return ResponseEntity.accepted().build();
    }

    /**
     * Lists and filters documents within a specific bucket.
     * This endpoint uses a POST request to accommodate a complex {@link FilterRequest} object in the request body,
     * which allows for flexible filtering, sorting, and pagination.
     *
     * @param gxBucketId    The identifier of the bucket to list documents from.
     * @param filterRequest The request body containing filtering, pagination, and sorting parameters.
     *
     * @return A {@link ResponseEntity} containing a {@link Page} of {@link DocumentProcessingView} objects.
     */
    @PostMapping("/v1/list/{gxBucketId}")
    public ResponseEntity<Page<DocumentProcessingView>> listDocuments(
            @PathVariable("gxBucketId") final Integer gxBucketId, @RequestBody final FilterRequest filterRequest) {
        log.info("Received request to list documents for gxBucketId: {}", gxBucketId);
        filterRequest.appendCriteria("gxBucketId", Operator.EQUALS, String.valueOf(gxBucketId));
        Page<DocumentProcessingView> documents = documentProcessingViewService.filter(filterRequest);
        return ResponseEntity.ok(documents);
    }

    /**
     * Retrieves a summary of document counts grouped by status for a given list of bucket IDs.
     *
     * @param request The request body containing a list of gxBucketIds.
     *
     * @return A map where each key is a bucket ID and the value is a list of its status metrics, including a 'Total'.
     */
    @PostMapping("/v1/metrics")
    public ResponseEntity<Map<Integer, List<StatusMetricItem>>> getDocumentMetrics(
            @RequestBody final MetricsRequest request) {
        log.info("Received request to fetch metrics for gxBucketIds: {}", request.getGxBucketIds());
        Map<Integer, List<StatusMetricItem>> metrics = documentProcessingViewService.getMetricsForBuckets(
                request.getGxBucketIds());
        return ResponseEntity.ok(metrics);
    }
}