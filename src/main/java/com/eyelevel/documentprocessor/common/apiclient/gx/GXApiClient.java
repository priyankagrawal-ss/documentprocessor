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
import java.util.Map;
import java.util.UUID;

/**
 * A specialized API client for interacting with the GroundX (GX) service.
 * This client handles document uploads, status checks, and bucket creation.
 */
@Slf4j
@Service("gxApiClient")
public class GXApiClient extends ApiClient {

    private final JsonParser jsonParser;

    @Value("${app.gx-client.endpoint.upload-file}")
    private String uploadDocumentEndpoint;

    @Value("${app.gx-client.endpoint.fetch-status}")
    private String fetchStatusEndpoint;

    @Value("${app.gx-client.endpoint.create-bucket}")
    private String createBucketEndpoint;

    /**
     * Constructs the GXApiClient with all required dependencies.
     *
     * @param webClient      The reactive {@link WebClient} for making HTTP requests.
     * @param authentication The authentication strategy for signing requests.
     * @param headerConfig   A configuration object for any additional, static headers.
     * @param jsonParser     A utility for parsing JSON responses.
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
     * Initiates the upload of a document to GroundX by providing its metadata and source URL.
     *
     * @param params The parameters required for the document upload.
     * @return The response from GX, including the process ID for tracking.
     * @throws ApiException if the API call returns a client or server error.
     */
    public GXUploadDocumentResponse uploadDocument(final GXDocumentUploadParameters params) {
        log.info("Requesting document upload to GX for file '{}' in bucket {}", params.fileName(), params.bucketId());
        try {
            GXUploadFileRequest.DocumentRequest docRequest = new GXUploadFileRequest.DocumentRequest(
                    params.bucketId(), params.fileName(), params.fileType(), params.sourceUrl()
            );
            GXUploadFileRequest payload = new GXUploadFileRequest(Collections.singletonList(docRequest));

            ApiRequest apiRequest = ApiRequest.builder()
                    .method(HttpMethod.POST)
                    .path(uploadDocumentEndpoint)
                    .body(payload)
                    .contentType(MediaType.APPLICATION_JSON)
                    .acceptMediaType(MediaType.APPLICATION_JSON)
                    .build();

            ApiResponse apiResponse = call(apiRequest);
            return jsonParser.parseObject(apiResponse.getData(), GXUploadDocumentResponse.class);

        } catch (ApiException e) {
            log.warn("API error during GX document upload for file '{}': {}", params.fileName(), e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("An unexpected error occurred while uploading document '{}' to bucket {}.", params.fileName(), params.bucketId(), e);
            throw new InternalServerException("Unexpected error during document upload to GX.");
        }
    }

    /**
     * Creates a new bucket in GroundX.
     *
     * @param bucketName The desired name for the new bucket.
     * @return The details of the newly created {@link GXBucket}.
     * @throws ApiException if the API call fails.
     */
    public GXBucket createGXBucket(final String bucketName) {
        log.info("Requesting to create a new GX bucket named '{}'", bucketName);
        try {
            ApiRequest apiRequest = ApiRequest.builder()
                    .method(HttpMethod.POST)
                    .path(createBucketEndpoint)
                    .body(Map.of("name", bucketName))
                    .contentType(MediaType.APPLICATION_JSON)
                    .acceptMediaType(MediaType.APPLICATION_JSON)
                    .build();

            ApiResponse apiResponse = call(apiRequest);
            return jsonParser.parseObject(apiResponse.getData(), GXBucket.class);
        } catch (ApiException e) {
            log.warn("API error while creating GX bucket for name '{}': {}", bucketName, e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("An unexpected error occurred while creating the GX bucket for name '{}'.", bucketName, e);
            throw new InternalServerException("Unexpected error during GX bucket creation.");
        }
    }

    /**
     * Fetches the current ingestion status of a previously uploaded document.
     *
     * @param processId The unique identifier of the ingestion process.
     * @return An {@link IngestResponse} containing the detailed status.
     * @throws ApiException if the API call fails.
     */
    public IngestResponse fetchUploadDocumentStatus(final UUID processId) {
        log.info("Fetching GX ingestion status for process ID: {}", processId);
        try {
            ApiRequest apiRequest = ApiRequest.builder()
                    .method(HttpMethod.GET)
                    .path(fetchStatusEndpoint)
                    .pathVariables(Map.of("processId", processId))
                    .acceptMediaType(MediaType.APPLICATION_JSON)
                    .build();

            ApiResponse apiResponse = call(apiRequest);
            return jsonParser.parseObject(apiResponse.getData(), IngestResponse.class);
        } catch (ApiException e) {
            log.warn("API error while fetching status for process ID '{}': {}", processId, e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("An unexpected error occurred while fetching status for process ID '{}'.", processId, e);
            throw new InternalServerException("Unexpected error while fetching process status from GX.");
        }
    }
}