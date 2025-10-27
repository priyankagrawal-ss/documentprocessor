package com.eyelevel.documentprocessor.controller;

import com.eyelevel.documentprocessor.dto.common.ApiResponse;
import com.eyelevel.documentprocessor.dto.metric.request.MetricsRequest;
import com.eyelevel.documentprocessor.dto.metric.response.StatusMetricItem;
import com.eyelevel.documentprocessor.dto.presign.download.PresignedDownloadResponse;
import com.eyelevel.documentprocessor.dto.presign.request.DownloadFileRequest;
import com.eyelevel.documentprocessor.dto.retry.request.RetryRequest;
import com.eyelevel.documentprocessor.dto.terminate.TerminateAllResponse;
import com.eyelevel.documentprocessor.dto.uploadfile.direct.PresignedUploadResponse;
import com.eyelevel.documentprocessor.dto.uploadfile.multipart.CompleteMultipartUploadRequest;
import com.eyelevel.documentprocessor.dto.uploadfile.multipart.InitiateMultipartUploadResponse;
import com.eyelevel.documentprocessor.dto.uploadfile.multipart.PresignedUrlPartResponse;
import com.eyelevel.documentprocessor.service.file.DownloadService;
import com.eyelevel.documentprocessor.service.file.RetryService;
import com.eyelevel.documentprocessor.service.file.view.DocumentProcessingViewService;
import com.eyelevel.documentprocessor.service.job.JobLifecycleManager;
import com.eyelevel.documentprocessor.service.job.JobOrchestrationService;
import com.eyelevel.documentprocessor.view.DocumentProcessingView;
import com.smartsensesolutions.commons.dao.filter.FilterRequest;
import com.smartsensesolutions.commons.dao.operator.Operator;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
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
@Validated // Enables validation for path variables and request parameters
public class DocumentProcessingController implements DocumentProcessingApi {

    private final JobOrchestrationService jobOrchestrationService;
    private final DocumentProcessingViewService documentProcessingViewService;
    private final JobLifecycleManager jobLifecycleManager;
    private final RetryService retryService;
    private final DownloadService downloadService;
    // --- 1. UPLOAD ENDPOINTS ---

    @Override
    @PostMapping("/v1/uploads/direct")
    public ResponseEntity<ApiResponse<PresignedUploadResponse>> createDirectUploadUrl(
            @RequestParam("fileName") @NotBlank(message = "The 'fileName' parameter cannot be empty.") final String fileName,
            @RequestParam(value = "gxBucketId", required = false) @Positive(message = "The 'gxBucketId' must be a positive number.") final Integer gxBucketId,
            @RequestParam(value = "skipGxProcess", defaultValue = "false") final boolean skipGxProcess) {
        PresignedUploadResponse responseData = jobOrchestrationService.createJobAndPresignedUrl(fileName, gxBucketId, skipGxProcess);
        return ResponseEntity.ok(ApiResponse.success(responseData, "Pre-signed URL for direct upload generated successfully."));
    }

    @Override
    @PostMapping("/v1/uploads/multipart")
    public ResponseEntity<ApiResponse<InitiateMultipartUploadResponse>> initiateMultipartUpload(
            @RequestParam("fileName") @NotBlank(message = "The 'fileName' parameter cannot be empty.") final String fileName,
            @RequestParam(value = "gxBucketId", required = false) @Positive(message = "The 'gxBucketId' must be a positive number.") final Integer gxBucketId,
            @RequestParam(value = "skipGxProcess", defaultValue = "false") final boolean skipGxProcess) {
        InitiateMultipartUploadResponse responseData = jobOrchestrationService.createJobAndInitiateMultipartUpload(fileName, gxBucketId, skipGxProcess);
        return ResponseEntity.ok(ApiResponse.success(responseData, "Multipart upload initiated successfully."));
    }

    @Override
    @GetMapping("/v1/uploads/{jobId}/parts/{partNumber}")
    public ResponseEntity<ApiResponse<PresignedUrlPartResponse>> getPresignedUrlForPart(
            @PathVariable @Positive(message = "The 'jobId' must be a positive number.") final Long jobId,
            @PathVariable @Min(value = 1, message = "The 'partNumber' must be at least 1.") @Max(value = 10000, message = "The 'partNumber' cannot exceed 10000.") final int partNumber,
            @RequestParam("uploadId") @NotBlank(message = "The 'uploadId' parameter cannot be empty.") final String uploadId) {
        PresignedUrlPartResponse responseData = new PresignedUrlPartResponse(
                jobOrchestrationService.generatePresignedUrlForPart(jobId, uploadId, partNumber));
        return ResponseEntity.ok(ApiResponse.success(responseData));
    }

    @Override
    @PostMapping("/v1/uploads/{jobId}/complete")
    public ResponseEntity<ApiResponse<Void>> completeMultipartUpload(
            @PathVariable @Positive(message = "The 'jobId' must be a positive number.") final Long jobId,
            @RequestBody @Valid final CompleteMultipartUploadRequest request) {
        jobOrchestrationService.completeMultipartUpload(jobId, request.getUploadId(), request.getParts());
        return ResponseEntity.ok(ApiResponse.success(null, "Multipart upload completed successfully."));
    }

    // --- 2. JOB LIFECYCLE ENDPOINTS ---

    @Override
    @PostMapping("/v1/jobs/{jobId}/trigger-processing")
    public ResponseEntity<ApiResponse<Void>> startProcessing(@PathVariable @Positive(message = "The 'jobId' must be a positive number.") final Long jobId) {
        jobOrchestrationService.triggerProcessing(jobId);
        return new ResponseEntity<>(ApiResponse.success(null, "Processing has been accepted and queued."), HttpStatus.ACCEPTED);
    }

    @Override
    @PostMapping("/v1/jobs/retry")
    public ResponseEntity<ApiResponse<Void>> retryFailedTask(@Valid @RequestBody final RetryRequest retryRequest) {
        retryService.retryFailedProcess(retryRequest);
        return new ResponseEntity<>(ApiResponse.success(null, "Retry request accepted and the task has been re-queued."), HttpStatus.ACCEPTED);
    }

    @Override
    @PostMapping("/v1/jobs/{jobId}/terminate")
    public ResponseEntity<ApiResponse<Void>> terminateJob(@PathVariable @Positive(message = "The 'jobId' must be a positive number.") final Long jobId) {
        jobLifecycleManager.terminateJob(jobId);
        return new ResponseEntity<>(ApiResponse.success(null, "Termination request for job ID " + jobId + " has been accepted."), HttpStatus.ACCEPTED);
    }

    @Override
    @PostMapping("/v1/jobs/terminate-all-active")
    public ResponseEntity<ApiResponse<TerminateAllResponse>> terminateAllActiveJobs() {
        int terminatedCount = jobLifecycleManager.terminateAllActiveJobs();
        String message = String.format("Termination signal sent to %d active jobs and queues have been purged.", terminatedCount);
        TerminateAllResponse responseData = new TerminateAllResponse(message, terminatedCount);
        return ResponseEntity.ok(ApiResponse.success(responseData));
    }

    // --- 3. VIEW AND METRICS ENDPOINTS ---

    @Override
    @PostMapping("/v1/views/list/{gxBucketId}")
    public ResponseEntity<ApiResponse<Page<DocumentProcessingView>>> listDocuments(
            @PathVariable("gxBucketId") @Positive(message = "The 'gxBucketId' must be a positive number.") final Integer gxBucketId,
            @RequestBody @Valid final FilterRequest filterRequest) {
        filterRequest.appendCriteria("gxBucketId", Operator.EQUALS, String.valueOf(gxBucketId));
        Page<DocumentProcessingView> documents = documentProcessingViewService.filter(filterRequest);
        return ResponseEntity.ok(ApiResponse.success(documents));
    }

    @Override
    @PostMapping("/v1/views/metrics")
    public ResponseEntity<ApiResponse<Map<Integer, List<StatusMetricItem>>>> getDocumentMetrics(
            @RequestBody @Valid final MetricsRequest request) {
        Map<Integer, List<StatusMetricItem>> metrics = documentProcessingViewService.getMetricsForBuckets(request.getGxBucketIds());
        return ResponseEntity.ok(ApiResponse.success(metrics));
    }

    @Override
    @PostMapping("/v1/downloads/presigned-url")
    public ResponseEntity<ApiResponse<PresignedDownloadResponse>> generatePresignedDownloadUrl(@Valid @RequestBody final DownloadFileRequest request) {
        PresignedDownloadResponse responseData = downloadService.generatePresignedDownloadUrl(request);
        return ResponseEntity.ok(ApiResponse.success(responseData));
    }
}