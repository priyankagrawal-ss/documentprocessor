package com.eyelevel.documentprocessor.common.apiclient.gx;

import com.eyelevel.documentprocessor.common.apiclient.ApiClient;
import com.eyelevel.documentprocessor.common.apiclient.authentication.Authentication;
import com.eyelevel.documentprocessor.common.apiclient.model.ApiRequest;
import com.eyelevel.documentprocessor.common.apiclient.model.ApiResponse;
import com.eyelevel.documentprocessor.common.apiclient.model.HeaderConfig;
import com.eyelevel.documentprocessor.common.json.JsonParser;
import com.eyelevel.documentprocessor.dto.gx.creategxbucket.response.GXBucket;
import com.eyelevel.documentprocessor.dto.gx.docupload.GXDocumentUploadParameters;
import com.eyelevel.documentprocessor.dto.gx.docupload.request.GXUploadFileRequest;
import com.eyelevel.documentprocessor.dto.gx.docupload.response.GXUploadDocumentResponse;
import com.eyelevel.documentprocessor.dto.gx.uploadstatus.response.IngestResponse;
import com.eyelevel.documentprocessor.exception.InternalServerException;
import com.eyelevel.documentprocessor.exception.apiclient.ApiException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Service for interacting with the GroundX (GX) API.
 */
@Slf4j
@Service("gxApiClient")
public class GXApiClient extends ApiClient {

    private final JsonParser jsonParser;

    @Value("${app.gx-client.endpoint.upload-file}")
    private String uploadDocumentGXEndpoint;

    @Value("${app.gx-client.endpoint.fetch-status}")
    private String fetchUploadDocumentStatusEndpoint;

    @Value("${app.gx-client.endpoint.create-bucket}")
    private String createGXBucketEndpoint;

    /**
     * Constructs a new GXApiClient.
     *
     * @param webClient      The WebClient for making HTTP requests.
     * @param authentication The authentication provider for GX.
     * @param headerConfig   The header configuration for API requests.
     * @param jsonParser     The JSON parser for serializing and deserializing objects.
     */
    public GXApiClient(
            @Qualifier("gxWebClient") final WebClient webClient,
            @Qualifier("gxAuthentication") final Authentication authentication,
            @Qualifier("gxHeader") final HeaderConfig headerConfig,
            @Qualifier("jacksonJsonParser") final JsonParser jsonParser
    ) {
        super(webClient, authentication, headerConfig);
        this.jsonParser = jsonParser;
    }

    /**
     * Uploads a document to GX.
     *
     * @param gxDocUploadParams The parameters for the document upload.
     * @return The response from the upload request.
     * @throws ApiException            if the API call returns an error.
     * @throws InternalServerException if an unexpected error occurs during the process.
     */
    public GXUploadDocumentResponse uploadDocument(final GXDocumentUploadParameters gxDocUploadParams) {
        try {
            final GXUploadFileRequest gxUploadFileRequest = getGxUploadFileRequest(gxDocUploadParams);
            final ApiRequest apiRequest = prepareUploadDocumentRequest(gxUploadFileRequest);
            final ApiResponse apiResponse = call(apiRequest);

            return jsonParser.parseObject(apiResponse.getData(), GXUploadDocumentResponse.class);

        } catch (final ApiException e) {
            log.warn(
                    "API error occurred while uploading document '{}' to bucket {}.",
                    gxDocUploadParams.fileName(),
                    gxDocUploadParams.bucketId(),
                    e
            );
            throw e;
        } catch (final Exception e) {
            log.error(
                    "An unexpected error occurred while uploading document '{}' to bucket {}.",
                    gxDocUploadParams.fileName(),
                    gxDocUploadParams.bucketId(),
                    e
            );
            throw new InternalServerException("An unexpected error occurred during document upload.");
        }
    }

    private static GXUploadFileRequest getGxUploadFileRequest(GXDocumentUploadParameters gxDocUploadParams) {
        final List<GXUploadFileRequest.DocumentRequest> documentRequests = Collections.singletonList(
                new GXUploadFileRequest.DocumentRequest(
                        gxDocUploadParams.bucketId(),
                        gxDocUploadParams.fileName(),
                        gxDocUploadParams.fileType(),
                        gxDocUploadParams.sourceUrl()
                )
        );

        return new GXUploadFileRequest(documentRequests);
    }

    /**
     * Creates a new bucket in GX.
     *
     * @param claimName The name of the claim to create the bucket for.
     * @return The details of the created bucket.
     * @throws ApiException            if the API call fails.
     * @throws InternalServerException if an unexpected error occurs.
     */
    public GXBucket createGXBucket(final String claimName) {
        try {
            final ApiRequest apiRequest = prepareCreateBucketRequest(claimName);
            final ApiResponse apiResponse = call(apiRequest);
            return jsonParser.parseObject(apiResponse.getData(), GXBucket.class);
        } catch (final ApiException e) {
            log.warn("Error while creating bucket for claim name: {} in GroundX API.", claimName, e);
            throw e;
        } catch (final Exception e) {
            log.error("An unexpected error occurred while creating the bucket for claim: {}.", claimName, e);
            throw new InternalServerException("An unexpected error occurred while creating the bucket.");
        }
    }

    /**
     * Fetches the status of a document upload process.
     *
     * @param processId The ID of the process to fetch the status for.
     * @return The ingestion response details.
     * @throws InternalServerException if an unexpected error occurs.
     */
    public IngestResponse fetchUploadDocumentStatus(final UUID processId) {
        try {
            log.info("Fetching status for process id {}", processId);
            final ApiRequest apiRequest = prepareProcessStatusRequest(processId);
            final ApiResponse apiResponse = call(apiRequest);
            return jsonParser.parseObject(apiResponse.getData(), IngestResponse.class);
        } catch (final Exception e) {
            log.error("An unexpected error occurred while fetching process status for processId {}", processId, e);
            throw new InternalServerException("An unexpected error occurred while fetching process status from GX.");
        }
    }

    private ApiRequest prepareCreateBucketRequest(final String claimName) {
        return ApiRequest.builder()
                .method(HttpMethod.POST)
                .path(createGXBucketEndpoint)
                .body(Map.of("name", claimName))
                .contentType(MediaType.APPLICATION_JSON)
                .acceptMediaType(MediaType.APPLICATION_JSON)
                .build();
    }

    private ApiRequest prepareProcessStatusRequest(final UUID processId) {
        return ApiRequest.builder()
                .method(HttpMethod.GET)
                .path(fetchUploadDocumentStatusEndpoint)
                .pathVariables(Map.of("processId", processId))
                .contentType(MediaType.APPLICATION_JSON)
                .acceptMediaType(MediaType.APPLICATION_JSON)
                .build();
    }

    private ApiRequest prepareUploadDocumentRequest(final GXUploadFileRequest gxUploadFileRequest) {
        return ApiRequest.builder()
                .method(HttpMethod.POST)
                .path(uploadDocumentGXEndpoint)
                .body(gxUploadFileRequest)
                .contentType(MediaType.APPLICATION_JSON)
                .acceptMediaType(MediaType.APPLICATION_JSON)
                .build();
    }
}