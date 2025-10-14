package com.eyelevel.documentprocessor.common.apiclient.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.lang.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a request to an external API.
 *
 * <p>This class encapsulates all the information needed to construct and send an API request,
 * including the HTTP method, path, query parameters, headers, and request body. It utilizes the
 * Lombok annotations `@Builder` for easy construction and `@Data` for automatic generation of
 * boilerplate code (getters, setters, equals, hashCode, toString).
 */
@Builder
@Data
public class ApiRequest {

    /**
     * The HTTP method for the API request (e.g., GET, POST, PUT, DELETE).
     */
    private final HttpMethod method;

    /**
     * The path (endpoint) of the API request.
     */
    private final String path;

    /**
     * Optional query parameters to be included in the API request.
     *
     * <p>This field can be null if no query parameters are needed.
     */
    @Nullable
    private final Map<String, Object> queryParams;

    /**
     * Optional path variables to be included in the API request.
     *
     * <p>This field can be null if no path variables are needed.
     */
    @Nullable
    private final Map<String, Object> pathVariables;

    /**
     * The headers for the API request.
     *
     * <p>This field is initialized as a HashMap to allow for easy addition and modification of
     * headers.
     */
    private final Map<String, String> headers = new HashMap<>();

    /**
     * The body of the API request.
     *
     * <p>This field can be null if no request body is needed.
     */
    @Nullable
    private final Object body;

    /**
     * The expected response type of the API request.
     *
     * <p>This field can be null if the response type is not explicitly needed or if the response is
     * a simple success/failure indication. Consider using a more specific type or removing if not
     * needed.
     */
    @Nullable
    private final Class<?> responseType;

    /**
     * Indicates whether the response is expected to be a stream.
     *
     * <p>If true, the response will be treated as a stream of data.
     */
    private final boolean isStream;

    /**
     * The media type that the client will accept in the response.
     *
     * <p>This field can be null if the client accepts any media type.
     */
    @Nullable
    private final MediaType acceptMediaType;

    /**
     * The content type of the request body.
     *
     * <p>This field can be null if there is no request body.
     */
    @Nullable
    private final MediaType contentType;
}
