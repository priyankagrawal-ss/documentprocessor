package com.eyelevel.documentprocessor.controller;

import com.eyelevel.documentprocessor.dto.PresignedUploadResponse;
import com.eyelevel.documentprocessor.service.JobOrchestrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST controller for handling document upload and processing requests.
 */
@RestController
@RequestMapping("/api/documents")
@RequiredArgsConstructor
@Slf4j
public class DocumentController {

    private final JobOrchestrationService jobOrchestrationService;

    /**
     * Stage 1: Creates a new processing job and generates a pre-signed URL for the client to upload a file.
     * This endpoint supports both single-context and bulk uploads. For a bulk upload, omit the gxBucketId parameter.
     *
     * @param fileName      The original name of the file to be uploaded.
     * @param gxBucketId    An identifier for the client's bucket. (Optional: omitting this designates a bulk upload).
     * @param skipGxProcess A flag to indicate if a subsequent processing step should be skipped.
     * @return A response entity containing the unique job ID and the pre-signed S3 URL.
     */
    @PostMapping("/generate-upload-url")
    public ResponseEntity<PresignedUploadResponse> generateUploadUrl(
            @RequestParam("fileName") String fileName,
            @RequestParam(value = "gxBucketId", required = false) Integer gxBucketId,
            @RequestParam(value = "skipGxProcess", defaultValue = "false") boolean skipGxProcess) {

        if (gxBucketId == null) {
            log.info("Received request to generate BULK upload URL for fileName: {}", fileName);
        } else {
            log.info("Received request to generate single upload URL for fileName: {}, gxBucketId: {}", fileName, gxBucketId);
        }
        PresignedUploadResponse response = jobOrchestrationService.createJobAndPresignedUrl(fileName, gxBucketId, skipGxProcess);
        log.info("Successfully generated pre-signed URL for new Job ID: {}", response.getJobId());
        return ResponseEntity.ok(response);
    }

    /**
     * Stage 2: Triggered by the client after it has successfully uploaded the file to S3.
     * This signals the application to begin backend processing.
     *
     * @param jobId The unique ID of the job, returned by the generate-upload-url endpoint.
     * @return An HTTP 202 Accepted response.
     */
    @PostMapping("/trigger-processing/{jobId}")
    public ResponseEntity<Void> triggerProcessing(@PathVariable Long jobId) {
        log.info("Received request to trigger processing for Job ID: {}", jobId);
        jobOrchestrationService.triggerProcessing(jobId);
        log.info("Processing trigger for Job ID: {} has been accepted and queued.", jobId);
        return ResponseEntity.accepted().build();
    }
}