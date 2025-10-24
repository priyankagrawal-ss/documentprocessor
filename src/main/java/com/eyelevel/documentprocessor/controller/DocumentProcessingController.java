package com.eyelevel.documentprocessor.controller;

import com.eyelevel.documentprocessor.dto.common.ApiResponse;
import com.eyelevel.documentprocessor.dto.metric.request.MetricsRequest;
import com.eyelevel.documentprocessor.dto.metric.response.StatusMetricItem;
import com.eyelevel.documentprocessor.dto.retry.request.RetryRequest;
import com.eyelevel.documentprocessor.dto.terminate.TerminateAllResponse;
import com.eyelevel.documentprocessor.dto.uploadfile.direct.PresignedUploadResponse;
import com.eyelevel.documentprocessor.dto.uploadfile.multipart.CompleteMultipartUploadRequest;
import com.eyelevel.documentprocessor.dto.uploadfile.multipart.InitiateMultipartUploadResponse;
import com.eyelevel.documentprocessor.dto.uploadfile.multipart.PresignedUrlPartResponse;
import com.eyelevel.documentprocessor.service.file.RetryService;
import com.eyelevel.documentprocessor.service.file.view.DocumentProcessingViewService;
import com.eyelevel.documentprocessor.service.job.JobLifecycleManager;
import com.eyelevel.documentprocessor.service.job.JobOrchestrationService;
import com.eyelevel.documentprocessor.view.DocumentProcessingView;
import com.smartsensesolutions.commons.dao.filter.FilterRequest;
import com.smartsensesolutions.commons.dao.operator.Operator;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST controller for managing the entire document processing workflow, from upload to finalization.
 * All responses from this controller follow the standardized ApiResponse format.
 */
@Slf4j
@RestController
@RequestMapping("/documents")
@RequiredArgsConstructor
public class DocumentProcessingController {

    private final JobOrchestrationService jobOrchestrationService;
    private final DocumentProcessingViewService documentProcessingViewService;
    private final JobLifecycleManager jobLifecycleManager;
    private final RetryService retryService;

    // --- 1. UPLOAD ENDPOINTS ---

    /**
     * Creates a job and a pre-signed URL for a direct, single-part file upload.
     * Used for smaller files.
     * @param fileName The original name of the file to be uploaded.
     * @param gxBucketId Optional identifier for grouping documents.
     * @param skipGxProcess Optional flag to bypass a specific processing step.
     * @return An ApiResponse containing the PresignedUploadResponse with the job ID and the direct upload URL.
     */
    @PostMapping("/v1/uploads/direct")
    public ResponseEntity<ApiResponse<PresignedUploadResponse>> createDirectUploadUrl(
            @RequestParam("fileName") final String fileName,
            @RequestParam(value = "gxBucketId", required = false) final Integer gxBucketId,
            @RequestParam(value = "skipGxProcess", defaultValue = "false") final boolean skipGxProcess) {
        PresignedUploadResponse responseData = jobOrchestrationService.createJobAndPresignedUrl(fileName, gxBucketId, skipGxProcess);
        return ResponseEntity.ok(ApiResponse.success(responseData, "Pre-signed URL for direct upload generated successfully."));
    }

    /**
     * Initiates a multipart upload for a large file, creating a job and returning an Upload ID.
     * This is the first step in the large file upload workflow.
     * @param fileName The original name of the file to be uploaded.
     * @param gxBucketId Optional identifier for grouping documents.
     * @param skipGxProcess Optional flag to bypass a specific processing step.
     * @return An ApiResponse containing the InitiateMultipartUploadResponse with the new job ID and S3 Upload ID.
     */
    @PostMapping("/v1/uploads/multipart")
    public ResponseEntity<ApiResponse<InitiateMultipartUploadResponse>> initiateMultipartUpload(
            @RequestParam("fileName") final String fileName,
            @RequestParam(value = "gxBucketId", required = false) final Integer gxBucketId,
            @RequestParam(value = "skipGxProcess", defaultValue = "false") final boolean skipGxProcess) {
        InitiateMultipartUploadResponse responseData = jobOrchestrationService.createJobAndInitiateMultipartUpload(fileName, gxBucketId, skipGxProcess);
        return ResponseEntity.ok(ApiResponse.success(responseData, "Multipart upload initiated successfully."));
    }

    /**
     * Generates a pre-signed URL for a single part (chunk) of a multipart upload.
     * @param jobId The ID of the job initiated in the first step.
     * @param partNumber The sequential number of the file chunk (starting at 1).
     * @param uploadId The S3 Upload ID returned from the initiation step.
     * @return An ApiResponse containing the PresignedUrlPartResponse with the temporary URL for the specified part.
     */
    @GetMapping("/v1/uploads/{jobId}/parts/{partNumber}")
    public ResponseEntity<ApiResponse<PresignedUrlPartResponse>> getPresignedUrlForPart(
            @PathVariable final Long jobId,
            @PathVariable final int partNumber,
            @RequestParam("uploadId") final String uploadId) {
        PresignedUrlPartResponse responseData = new PresignedUrlPartResponse(
                jobOrchestrationService.generatePresignedUrlForPart(jobId, uploadId, partNumber));
        return ResponseEntity.ok(ApiResponse.success(responseData));
    }

    /**
     * Completes a multipart upload after all parts have been successfully uploaded to S3.
     * @param jobId The ID of the job being completed.
     * @param request The request body containing the S3 Upload ID and the list of uploaded parts with their ETags.
     * @return A successful ApiResponse with no data payload.
     */
    @PostMapping("/v1/uploads/{jobId}/complete")
    public ResponseEntity<ApiResponse<Void>> completeMultipartUpload(
            @PathVariable final Long jobId,
            @RequestBody final CompleteMultipartUploadRequest request) {
        jobOrchestrationService.completeMultipartUpload(jobId, request.getUploadId(), request.getParts());
        return ResponseEntity.ok(ApiResponse.success(null, "Multipart upload completed successfully."));
    }

    // --- 2. JOB LIFECYCLE ENDPOINTS ---

    /**
     * Triggers the backend processing for a file that has been successfully uploaded.
     * @param jobId The unique identifier of the job to trigger.
     * @return A 202 Accepted ApiResponse indicating the processing request has been queued.
     */
    @PostMapping("/v1/jobs/{jobId}/trigger-processing")
    public ResponseEntity<ApiResponse<Void>> startProcessing(@PathVariable final Long jobId) {
        jobOrchestrationService.triggerProcessing(jobId);
        return new ResponseEntity<>(ApiResponse.success(null, "Processing has been accepted and queued."), HttpStatus.ACCEPTED);
    }

    /**
     * Retries a failed task. The client must provide either a 'fileMasterId' or 'gxMasterId'.
     * @param retryRequest The request body containing the ID of the failed task.
     * @return A 202 Accepted ApiResponse indicating the retry request has been queued.
     */
    @PostMapping("/v1/jobs/retry")
    public ResponseEntity<ApiResponse<Void>> retryFailedTask(@Valid @RequestBody final RetryRequest retryRequest) {
        retryService.retryFailedProcess(retryRequest);
        return new ResponseEntity<>(ApiResponse.success(null, "Retry request accepted and the task has been re-queued."), HttpStatus.ACCEPTED);
    }

    /**
     * [ADMIN] Terminates a single active job and attempts to stop all related processing.
     * @param jobId The unique identifier of the job to terminate.
     * @return A 202 Accepted ApiResponse indicating the termination request was accepted.
     */
    @PostMapping("/v1/jobs/{jobId}/terminate")
    public ResponseEntity<ApiResponse<Void>> terminateJob(@PathVariable final Long jobId) {
        jobLifecycleManager.terminateJob(jobId);
        return new ResponseEntity<>(ApiResponse.success(null, "Termination request for job ID " + jobId + " has been accepted."), HttpStatus.ACCEPTED);
    }

    /**
     * [ADMIN] Terminates ALL active jobs in the system and purges the SQS queues.
     * @return An ApiResponse containing a summary of the termination action.
     */
    @PostMapping("/v1/jobs/terminate-all-active")
    public ResponseEntity<ApiResponse<TerminateAllResponse>> terminateAllActiveJobs() {
        int terminatedCount = jobLifecycleManager.terminateAllActiveJobs();
        String message = String.format("Termination signal sent to %d active jobs and queues have been purged.", terminatedCount);
        TerminateAllResponse responseData = new TerminateAllResponse(message, terminatedCount);
        return ResponseEntity.ok(ApiResponse.success(responseData));
    }

    // --- 3. VIEW AND METRICS ENDPOINTS ---

    /**
     * Lists and filters documents for a specific bucket with pagination and sorting.
     * @param gxBucketId The identifier of the bucket to list documents from.
     * @param filterRequest The request body containing filtering, pagination, and sorting parameters.
     * @return An ApiResponse containing a Page of DocumentProcessingView objects.
     */
    @PostMapping("/v1/views/list/{gxBucketId}")
    public ResponseEntity<ApiResponse<Page<DocumentProcessingView>>> listDocuments(
            @PathVariable("gxBucketId") final Integer gxBucketId,
            @RequestBody final FilterRequest filterRequest) {
        filterRequest.appendCriteria("gxBucketId", Operator.EQUALS, String.valueOf(gxBucketId));
        Page<DocumentProcessingView> documents = documentProcessingViewService.filter(filterRequest);
        return ResponseEntity.ok(ApiResponse.success(documents));
    }

    /**
     * Retrieves a summary of document counts grouped by status for a given list of bucket IDs.
     * @param request The request body containing a list of gxBucketIds.
     * @return An ApiResponse containing a map where each key is a bucket ID and the value is a list of its status metrics.
     */
    @PostMapping("/v1/views/metrics")
    public ResponseEntity<ApiResponse<Map<Integer, List<StatusMetricItem>>>> getDocumentMetrics(
            @RequestBody final MetricsRequest request) {
        Map<Integer, List<StatusMetricItem>> metrics = documentProcessingViewService.getMetricsForBuckets(request.getGxBucketIds());
        return ResponseEntity.ok(ApiResponse.success(metrics));
    }
}