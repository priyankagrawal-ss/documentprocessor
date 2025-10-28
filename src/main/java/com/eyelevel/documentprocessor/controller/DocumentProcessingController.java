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
 * REST controller for managing document processing lifecycle,
 * including upload, processing, retry, termination, and metrics.
 * All responses follow the standardized {@link ApiResponse} format.
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
    
    // --- 1. UPLOAD ENDPOINTS ---

    @Override
    @PostMapping("/v1/uploads/direct")
    public ResponseEntity<ApiResponse<PresignedUploadResponse>> createDirectUploadUrl(
            @RequestParam("fileName") @NotBlank(message = "The 'fileName' parameter cannot be empty.") final String fileName,
            @RequestParam(value = "gxBucketId", required = false) @Positive(message = "The 'gxBucketId' must be a positive number.") final Integer gxBucketId,
            @RequestParam(value = "skipGxProcess", defaultValue = "false") final boolean skipGxProcess) {

        log.info("Creating direct upload URL for file: {}, gxBucketId: {}, skipGxProcess: {}", fileName, gxBucketId, skipGxProcess);

        PresignedUploadResponse responseData = jobOrchestrationService.createJobAndPresignedUrl(fileName, gxBucketId, skipGxProcess);

        ApiResponse<PresignedUploadResponse> response = ApiResponse.<PresignedUploadResponse>builder()
                .response(responseData)
                .displayMessage("Pre-signed URL for direct file upload generated successfully.")
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

        log.info("Initiating multipart upload for file: {}, gxBucketId: {}, skipGxProcess: {}", fileName, gxBucketId, skipGxProcess);

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

        log.debug("Generating pre-signed URL for jobId: {}, uploadId: {}, partNumber: {}", jobId, uploadId, partNumber);

        PresignedUrlPartResponse responseData = new PresignedUrlPartResponse(
                jobOrchestrationService.generatePresignedUrlForPart(jobId, uploadId, partNumber));

        ApiResponse<PresignedUrlPartResponse> response = ApiResponse.<PresignedUrlPartResponse>builder()
                .response(responseData)
                .displayMessage("Pre-signed URL for part upload generated successfully.")
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

        log.info("Completing multipart upload for jobId: {}, uploadId: {}", jobId, request.getUploadId());
        jobOrchestrationService.completeMultipartUpload(jobId, request.getUploadId(), request.getParts());

        ApiResponse<Void> response = ApiResponse.<Void>builder()
                .displayMessage("Multipart upload completed and file successfully assembled.")
                .showMessage(true)
                .statusCode(HttpStatus.OK.value())
                .build();

        return ResponseEntity.ok(response);
    }

    // --- 2. JOB LIFECYCLE ENDPOINTS ---

    @Override
    @PostMapping("/v1/jobs/{jobId}/trigger-processing")
    public ResponseEntity<ApiResponse<Void>> startProcessing(@PathVariable @Positive(message = "The 'jobId' must be a positive number.") final Long jobId) {
        log.info("Triggering processing for jobId: {}", jobId);
        jobOrchestrationService.triggerProcessing(jobId);

        ApiResponse<Void> response = ApiResponse.<Void>builder()
                .displayMessage("Processing started and job has been queued.")
                .showMessage(true)
                .statusCode(HttpStatus.ACCEPTED.value())
                .build();

        return new ResponseEntity<>(response, HttpStatus.ACCEPTED);
    }

    @Override
    @PostMapping("/v1/jobs/retry")
    public ResponseEntity<ApiResponse<Void>> retryFailedTask(@Valid @RequestBody final RetryRequest retryRequest) {
        log.warn("Retrying failed job with parameters: {}", retryRequest);
        retryService.retryFailedProcess(retryRequest);

        ApiResponse<Void> response = ApiResponse.<Void>builder()
                .displayMessage("Retry request accepted. The task has been re-queued for processing.")
                .showMessage(true)
                .statusCode(HttpStatus.ACCEPTED.value())
                .build();

        return new ResponseEntity<>(response, HttpStatus.ACCEPTED);
    }

    @Override
    @PostMapping("/v1/jobs/{jobId}/terminate")
    public ResponseEntity<ApiResponse<Void>> terminateJob(@PathVariable @Positive(message = "The 'jobId' must be a positive number.") final Long jobId) {
        log.warn("Terminating jobId: {}", jobId);
        jobLifecycleManager.terminateJob(jobId);

        ApiResponse<Void> response = ApiResponse.<Void>builder()
                .displayMessage("Termination signal sent for job ID " + jobId + ".")
                .showMessage(true)
                .statusCode(HttpStatus.ACCEPTED.value())
                .build();

        return new ResponseEntity<>(response, HttpStatus.ACCEPTED);
    }

    @Override
    @PostMapping("/v1/jobs/terminate-all-active")
    public ResponseEntity<ApiResponse<TerminateAllResponse>> terminateAllActiveJobs() {
        log.warn("Terminating all active jobs.");
        int terminatedCount = jobLifecycleManager.terminateAllActiveJobs();

        TerminateAllResponse responseData = new TerminateAllResponse(
                String.format("%d active job(s) terminated successfully.", terminatedCount),
                terminatedCount
        );

        ApiResponse<TerminateAllResponse> response = ApiResponse.<TerminateAllResponse>builder()
                .response(responseData)
                .displayMessage(responseData.getMessage())
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

        log.debug("Listing documents for gxBucketId: {}", gxBucketId);
        filterRequest.appendCriteria("gxBucketId", Operator.EQUALS, String.valueOf(gxBucketId));

        Page<DocumentProcessingView> documents = documentProcessingViewService.filter(filterRequest);

        ApiResponse<Page<DocumentProcessingView>> response = ApiResponse.<Page<DocumentProcessingView>>builder()
                .response(documents)
                .displayMessage("Document list retrieved successfully.")
                .showMessage(false)
                .statusCode(HttpStatus.OK.value())
                .build();

        return ResponseEntity.ok(response);
    }

    @Override
    @PostMapping("/v1/views/metrics")
    public ResponseEntity<ApiResponse<Map<Integer, List<StatusMetricItem>>>> getDocumentMetrics(
            @RequestBody @Valid final MetricsRequest request) {

        log.debug("Fetching document metrics for bucket IDs: {}", request.getGxBucketIds());
        Map<Integer, List<StatusMetricItem>> metrics = documentProcessingViewService.getMetricsForBuckets(request.getGxBucketIds());

        ApiResponse<Map<Integer, List<StatusMetricItem>>> response = ApiResponse.<Map<Integer, List<StatusMetricItem>>>builder()
                .response(metrics)
                .displayMessage("Metrics retrieved successfully.")
                .showMessage(false)
                .statusCode(HttpStatus.OK.value())
                .build();

        return ResponseEntity.ok(response);
    }

    @Override
    @PostMapping("/v1/downloads/presigned-url")
    public ResponseEntity<ApiResponse<PresignedDownloadResponse>> generatePresignedDownloadUrl(
            @Valid @RequestBody final DownloadFileRequest request) {

        log.info("Generating pre-signed download URL for: {}", request);
        PresignedDownloadResponse responseData = downloadService.generatePresignedDownloadUrl(request);

        ApiResponse<PresignedDownloadResponse> response = ApiResponse.<PresignedDownloadResponse>builder()
                .response(responseData)
                .displayMessage("Pre-signed download URL generated successfully.")
                .showMessage(true)
                .statusCode(HttpStatus.OK.value())
                .build();

        return ResponseEntity.ok(response);
    }
}
