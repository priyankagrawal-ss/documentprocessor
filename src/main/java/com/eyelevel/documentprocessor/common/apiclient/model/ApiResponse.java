package com.eyelevel.documentprocessor.common.apiclient.model;

import lombok.Builder;
import lombok.Getter;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.lang.Nullable;

import java.time.Instant;

/**
 * Represents the response from an external API call.
 *
 * <p>This class encapsulates the data returned by the API, along with metadata about the response,
 * such as the content type, headers, status code, and timestamp. It utilizes the Lombok
 * annotations `@Builder` for easy construction and `@Getter` for automatic generation of getters.
 */
@Builder
@Getter
public class ApiResponse {

    /**
     * The response data as a byte array.
     *
     * <p>This field can be null if there is no response data or if an error occurred.
     */
    @Nullable
    private final byte[] data;

    /**
     * The media type of the response.
     *
     * <p>This field can be null if the media type is not available or applicable.
     */
    @Nullable
    private final MediaType acceptType;

    /**
     * The HTTP headers from the response.
     *
     * <p>This field can be null if there are no headers in the response.
     */
    @Nullable
    private final HttpHeaders headers;

    /**
     * The HTTP status code of the response.
     */
    private final int statusCode;

    /**
     * The timestamp when the response was received.
     */
    private final Instant timestamp;

    /**
     * Any error that occurred during the API call.
     *
     * <p>This field can be null if the API call was successful.
     */
    @Nullable
    private final Exception error;
}
