package com.eyelevel.documentprocessor.dto.gx.creategxbucket.response;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.ZonedDateTime;

/**
 * Represents the response from a GX bucket creation request.
 *
 * @param bucket The details of the created bucket.
 */
public record GXBucket(Bucket bucket) {

    /**
     * Contains the detailed information of a GX bucket.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Bucket(
            @JsonProperty(value = "bucketId", required = true)
            Integer bucketId,

            @JsonProperty(value = "created")
            @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
            ZonedDateTime createdAt,

            @JsonProperty(value = "fileCount")
            Integer fileCount,

            @JsonProperty(value = "fileSize")
            String fileSize,

            @JsonProperty(value = "name")
            String name,

            String claimantName,
            String claimId,

            @JsonProperty(value = "updated")
            @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
            ZonedDateTime updatedAt
    ) {
    }
}