package com.eyelevel.documentprocessor.controller;

import com.eyelevel.documentprocessor.dto.PresignedUploadResponse;
import com.eyelevel.documentprocessor.service.JobOrchestrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Provides REST endpoints for initiating and managing the two-stage document upload process.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/documents") // Versioned API endpoint
@RequiredArgsConstructor
public class DocumentController {

    private final JobOrchestrationService jobOrchestrationService;

    /**
     * **Stage 1:** Creates a new processing job and returns a pre-signed S3 URL.
     * The client uses this URL to upload the file directly to S3, bypassing the application server.
     *
     * @param fileName      The original name of the file to be uploaded.
     * @param gxBucketId    (Optional) An identifier for the client's context bucket. If omitted, the job is treated as a bulk upload.
     * @param skipGxProcess (Optional) A flag to indicate if the final GroundX processing step should be skipped. Defaults to false.
     * @return A {@link ResponseEntity} containing the unique job ID and the pre-signed S3 URL.
     */
    @PostMapping("/upload-url")
    public ResponseEntity<PresignedUploadResponse> generateUploadUrl(
            @RequestParam("fileName") final String fileName,
            @RequestParam(value = "gxBucketId", required = false) final Integer gxBucketId,
            @RequestParam(value = "skipGxProcess", defaultValue = "false") final boolean skipGxProcess) {

        log.info("Request received to generate upload URL for file: '{}', gxBucketId: {}",
                fileName, gxBucketId == null ? "BULK" : gxBucketId);

        final PresignedUploadResponse response = jobOrchestrationService.createJobAndPresignedUrl(fileName, gxBucketId, skipGxProcess);
        log.info("Successfully generated pre-signed URL for new Job ID: {}", response.getJobId());
        return ResponseEntity.ok(response);
    }

    /**
     * **Stage 2:** Triggered by the client after it has successfully uploaded the file to S3.
     * This request signals the application to begin backend processing of the uploaded file.
     *
     * @param jobId The unique ID of the job, returned by the generate-upload-url endpoint.
     * @return An {@link ResponseEntity} with HTTP 202 (Accepted) to indicate the request has been queued.
     */
    @PostMapping("/{jobId}/trigger-processing")
    public ResponseEntity<Void> triggerProcessing(@PathVariable("jobId") final Long jobId) {
        log.info("Request received to trigger processing for Job ID: {}", jobId);
        jobOrchestrationService.triggerProcessing(jobId);
        log.info("Processing for Job ID: {} has been accepted and queued.", jobId);
        return ResponseEntity.accepted().build();
    }
}