package com.eyelevel.documentprocessor.dto.presign.download;

import io.swagger.v3.oas.annotations.media.Schema;

import java.net.URL;

/**
 * A DTO that encapsulates the response from the pre-signed download URL generation endpoint.
 *
 * @param downloadUrl The pre-signed S3 URL that the client must use to download the file.
 *                    This URL has a limited time-to-live (TTL) and grants temporary GET access.
 */
@Schema(description = "Contains the temporary, secure URL for downloading a file.")
public record PresignedDownloadResponse(
        @Schema(description = "The pre-signed S3 URL for file download.",
                example = "https://your-bucket.s3.region.amazonaws.com/...")
        URL downloadUrl
) {
}
