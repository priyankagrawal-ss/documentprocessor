package com.eyelevel.documentprocessor.dto;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.net.URL;

/**
 * A Data Transfer Object (DTO) that encapsulates the response from the
 * pre-signed URL generation endpoint.
 */
@Getter
@RequiredArgsConstructor
public class PresignedUploadResponse {

    /**
     * The unique identifier for the newly created processing job.
     */
    private final Long jobId;

    /**
     * The pre-signed S3 URL that the client must use to upload the file.
     * This URL has a limited time-to-live (TTL) and grants temporary PUT access.
     */
    private final URL uploadUrl;
}