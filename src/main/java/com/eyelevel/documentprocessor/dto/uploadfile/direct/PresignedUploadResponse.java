package com.eyelevel.documentprocessor.dto.uploadfile.direct;

import java.net.URL;

/**
 * A Data Transfer Object (DTO) that encapsulates the response from the
 * pre-signed URL generation endpoint.
 *
 * @param jobId     The unique identifier for the newly created processing job.
 * @param uploadUrl The pre-signed S3 URL that the client must use to upload the file.
 *                  This URL has a limited time-to-live (TTL) and grants temporary PUT access.
 */
public record PresignedUploadResponse(Long jobId, URL uploadUrl) {

}