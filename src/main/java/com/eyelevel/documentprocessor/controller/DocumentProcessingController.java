package com.eyelevel.documentprocessor.controller;

import com.eyelevel.documentprocessor.dto.PresignedUploadResponse;
import com.eyelevel.documentprocessor.service.JobOrchestrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Handles all operations related to document ingestion and processing workflow.
 * <p>
 * Stage 1: Generate a pre-signed upload URL for direct file upload to S3.
 * Stage 2: Trigger backend processing after upload completion.
 */
@Slf4j
@RestController
@RequestMapping("/documents")
@RequiredArgsConstructor
public class DocumentProcessingController {

    private final JobOrchestrationService jobOrchestrationService;

    /**
     * Stage 1: Creates a new document processing job and returns a pre-signed S3 upload URL.
     *
     * @param fileName      The original name of the file to be uploaded.
     * @param gxBucketId    Optional: Identifier for the client's context bucket (for grouping or bulk uploads).
     * @param skipGxProcess Optional: If true, skips the final GroundX processing stage.
     * @return Response containing the job ID and pre-signed S3 upload URL.
     */
    @PostMapping("/v1/upload-url")
    public ResponseEntity<PresignedUploadResponse> createUploadUrl(
            @RequestParam("fileName") final String fileName,
            @RequestParam(value = "gxBucketId", required = false) final Integer gxBucketId,
            @RequestParam(value = "skipGxProcess", defaultValue = "false") final boolean skipGxProcess) {

        log.info("Generating upload URL for file='{}', gxBucketId={}", fileName, gxBucketId == null ? "BULK" : gxBucketId);

        final PresignedUploadResponse response =
                jobOrchestrationService.createJobAndPresignedUrl(fileName, gxBucketId, skipGxProcess);

        log.info("Generated pre-signed URL for Job ID={}", response.getJobId());
        return ResponseEntity.ok(response);
    }

    /**
     * Stage 2: Trigger backend processing for a specific document upload job.
     *
     * @param jobId The unique Job ID returned during upload URL creation.
     * @return HTTP 202 (Accepted) once the job has been queued for processing.
     */
    @PostMapping("/v1/process")
    public ResponseEntity<Void> startProcessing(@RequestParam("jobId") final Long jobId) {
        log.info("Triggering processing for Job ID={}", jobId);

        jobOrchestrationService.triggerProcessing(jobId);

        log.info("Processing accepted and queued for Job ID={}", jobId);
        return ResponseEntity.accepted().build();
    }
}
