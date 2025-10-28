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
@Validated
public class DocumentProcessingController implements DocumentProcessingApi {

    private final JobOrchestrationService jobOrchestrationService;
    private final DocumentProcessingViewService documentProcessingViewService;
    private final JobLifecycleManager jobLifecycleManager;
    private final RetryService retryService;
    private final DownloadService downloadService;

    private static final String DEFAULT_SUCCESS_MESSAGE = "Request was successful.";

    // --- 1. UPLOAD ENDPOINTS ---

    @Override
    @PostMapping("/v1/uploads/direct")
    public ResponseEntity<ApiResponse<PresignedUploadResponse>> createDirectUploadUrl(
            @RequestParam("fileName") @NotBlank(message = "The 'fileName' parameter cannot be empty.") final String fileName,
            @RequestParam(value = "gxBucketId", required = false) @Positive(message = "The 'gxBucketId' must be a positive number.") final Integer gxBucketId,
            @RequestParam(value = "skipGxProcess", defaultValue = "false") final boolean skipGxProcess) {
        PresignedUploadResponse responseData = jobOrchestrationService.createJobAndPresignedUrl(fileName, gxBucketId, skipGxProcess);
        ApiResponse<PresignedUploadResponse> response = ApiResponse.<PresignedUploadResponse>builder()
                .response(responseData)
                .displayMessage("Pre-signed URL for direct upload generated successfully.")
                .showMessage(true)
                .statusCode(HttpStatus.OK.value())
                .build();
        return ResponseEntity.ok(response);
    }

    @Override
    @PostMapping("/v1/uploads/multipart")
    public ResponseEntity<ApiResponse<InitiateMultipartUploadResponse>> initiateMultipartUpload(
            @RequestParam("fileName") @NotBlank(message = "The 'fileName' parameter cannot be empty.") final String fileName,
            @RequestParam(value = "gxBucketId", required = false) @Positive(message = "The 'gxBucketId' must be a positive number.") final Integer gxBucketId,
            @RequestParam(value = "skipGxProcess", defaultValue = "false") final boolean skipGxProcess) {
        InitiateMultipartUploadResponse responseData = jobOrchestrationService.createJobAndInitiateMultipartUpload(fileName, gxBucketId, skipGxProcess);
        ApiResponse<InitiateMultipartUploadResponse> response = ApiResponse.<InitiateMultipartUploadResponse>builder()
                .response(responseData)
                .displayMessage("Multipart upload initiated successfully.")
                .showMessage(true)
                .statusCode(HttpStatus.OK.value())
                .build();
        return ResponseEntity.ok(response);
    }

    @Override
    @GetMapping("/v1/uploads/{jobId}/parts/{partNumber}")
    public ResponseEntity<ApiResponse<PresignedUrlPartResponse>> getPresignedUrlForPart(
            @PathVariable @Positive(message = "The 'jobId' must be a positive number.") final Long jobId,
            @PathVariable @Min(value = 1, message = "The 'partNumber' must be at least 1.") @Max(value = 10000, message = "The 'partNumber' cannot exceed 10000.") final int partNumber,
            @RequestParam("uploadId") @NotBlank(message = "The 'uploadId' parameter cannot be empty.") final String uploadId) {
        PresignedUrlPartResponse responseData = new PresignedUrlPartResponse(
                jobOrchestrationService.generatePresignedUrlForPart(jobId, uploadId, partNumber));
        ApiResponse<PresignedUrlPartResponse> response = ApiResponse.<PresignedUrlPartResponse>builder()
                .response(responseData)
                .displayMessage(DEFAULT_SUCCESS_MESSAGE)
                .showMessage(true)
                .statusCode(HttpStatus.OK.value())
                .build();
        return ResponseEntity.ok(response);
    }

    @Override
    @PostMapping("/v1/uploads/{jobId}/complete")
    public ResponseEntity<ApiResponse<Void>> completeMultipartUpload(
            @PathVariable @Positive(message = "The 'jobId' must be a positive number.") final Long jobId,
            @RequestBody @Valid final CompleteMultipartUploadRequest request) {
        jobOrchestrationService.completeMultipartUpload(jobId, request.getUploadId(), request.getParts());
        ApiResponse<Void> response = ApiResponse.<Void>builder()
                .displayMessage("Multipart upload completed successfully.")
                .showMessage(true)
                .statusCode(HttpStatus.OK.value())
                .build();
        return ResponseEntity.ok(response);
    }

    // --- 2. JOB LIFECYCLE ENDPOINTS ---

    @Override
    @PostMapping("/v1/jobs/{jobId}/trigger-processing")
    public ResponseEntity<ApiResponse<Void>> startProcessing(@PathVariable @Positive(message = "The 'jobId' must be a positive number.") final Long jobId) {
        jobOrchestrationService.triggerProcessing(jobId);
        ApiResponse<Void> response = ApiResponse.<Void>builder()
                .displayMessage("Processing has been accepted and queued.")
                .showMessage(true)
                .statusCode(HttpStatus.ACCEPTED.value())
                .build();
        return new ResponseEntity<>(response, HttpStatus.ACCEPTED);
    }

    @Override
    @PostMapping("/v1/jobs/retry")
    public ResponseEntity<ApiResponse<Void>> retryFailedTask(@Valid @RequestBody final RetryRequest retryRequest) {
        retryService.retryFailedProcess(retryRequest);
        ApiResponse<Void> response = ApiResponse.<Void>builder()
                .displayMessage("Retry request accepted and the task has been re-queued.")
                .showMessage(true)
                .statusCode(HttpStatus.ACCEPTED.value())
                .build();
        return new ResponseEntity<>(response, HttpStatus.ACCEPTED);
    }

    @Override
    @PostMapping("/v1/jobs/{jobId}/terminate")
    public ResponseEntity<ApiResponse<Void>> terminateJob(@PathVariable @Positive(message = "The 'jobId' must be a positive number.") final Long jobId) {
        jobLifecycleManager.terminateJob(jobId);
        ApiResponse<Void> response = ApiResponse.<Void>builder()
                .displayMessage("Termination request for job ID " + jobId + " has been accepted.")
                .showMessage(true)
                .statusCode(HttpStatus.ACCEPTED.value())
                .build();
        return new ResponseEntity<>(response, HttpStatus.ACCEPTED);
    }

    @Override
    @PostMapping("/v1/jobs/terminate-all-active")
    public ResponseEntity<ApiResponse<TerminateAllResponse>> terminateAllActiveJobs() {
        int terminatedCount = jobLifecycleManager.terminateAllActiveJobs();
        String message = String.format("Termination signal sent to %d active jobs and queues have been purged.", terminatedCount);
        TerminateAllResponse responseData = new TerminateAllResponse(message, terminatedCount);
        ApiResponse<TerminateAllResponse> response = ApiResponse.<TerminateAllResponse>builder()
                .response(responseData)
                .displayMessage(DEFAULT_SUCCESS_MESSAGE)
                .showMessage(true)
                .statusCode(HttpStatus.OK.value())
                .build();
        return ResponseEntity.ok(response);
    }

    // --- 3. VIEW AND METRICS ENDPOINTS ---

    @Override
    @PostMapping("/v1/views/list/{gxBucketId}")
    public ResponseEntity<ApiResponse<Page<DocumentProcessingView>>> listDocuments(
            @PathVariable("gxBucketId") @Positive(message = "The 'gxBucketId' must be a positive number.") final Integer gxBucketId,
            @RequestBody @Valid final FilterRequest filterRequest) {
        filterRequest.appendCriteria("gxBucketId", Operator.EQUALS, String.valueOf(gxBucketId));
        Page<DocumentProcessingView> documents = documentProcessingViewService.filter(filterRequest);
        ApiResponse<Page<DocumentProcessingView>> response = ApiResponse.<Page<DocumentProcessingView>>builder()
                .response(documents)
                .displayMessage(DEFAULT_SUCCESS_MESSAGE)
                .showMessage(true)
                .statusCode(HttpStatus.OK.value())
                .build();
        return ResponseEntity.ok(response);
    }

    @Override
    @PostMapping("/v1/views/metrics")
    public ResponseEntity<ApiResponse<Map<Integer, List<StatusMetricItem>>>> getDocumentMetrics(
            @RequestBody @Valid final MetricsRequest request) {
        Map<Integer, List<StatusMetricItem>> metrics = documentProcessingViewService.getMetricsForBuckets(request.getGxBucketIds());
        ApiResponse<Map<Integer, List<StatusMetricItem>>> response = ApiResponse.<Map<Integer, List<StatusMetricItem>>>builder()
                .response(metrics)
                .displayMessage(DEFAULT_SUCCESS_MESSAGE)
                .showMessage(true)
                .statusCode(HttpStatus.OK.value())
                .build();
        return ResponseEntity.ok(response);
    }

    @Override
    @PostMapping("/v1/downloads/presigned-url")
    public ResponseEntity<ApiResponse<PresignedDownloadResponse>> generatePresignedDownloadUrl(@Valid @RequestBody final DownloadFileRequest request) {
        PresignedDownloadResponse responseData = downloadService.generatePresignedDownloadUrl(request);
        ApiResponse<PresignedDownloadResponse> response = ApiResponse.<PresignedDownloadResponse>builder()
                .response(responseData)
                .displayMessage(DEFAULT_SUCCESS_MESSAGE)
                .showMessage(true)
                .statusCode(HttpStatus.OK.value())
                .build();
        return ResponseEntity.ok(response);
    }
}