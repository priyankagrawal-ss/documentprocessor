package com.eyelevel.documentprocessor.common.apiclient;

import com.eyelevel.documentprocessor.common.apiclient.authentication.Authentication;
import com.eyelevel.documentprocessor.common.apiclient.model.ApiRequest;
import com.eyelevel.documentprocessor.common.apiclient.model.ApiResponse;
import com.eyelevel.documentprocessor.common.apiclient.model.HeaderConfig;
import com.eyelevel.documentprocessor.exception.apiclient.InternalServerException;
import com.eyelevel.documentprocessor.exception.apiclient.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.lang.NonNull;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.*;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.ConnectException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Abstract base class for API clients, providing common functionality for making API calls,
 * handling responses, and mapping exceptions. Subclasses should extend this class and configure
 * the {@link WebClient}, {@link Authentication}, and {@link HeaderConfig} as needed.
 */
@RequiredArgsConstructor
@Slf4j
public abstract class ApiClient {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    protected final WebClient webClient;
    protected final Authentication authentication;
    protected final HeaderConfig headerConfig;

    /**
     * Executes an API call based on the provided {@link ApiRequest}. This method configures the
     * request, applies authentication and headers, sends the request, handles the response, and maps
     * any exceptions.
     *
     * @param apiRequest The API request to execute. Must not be null.
     *
     * @return The API response.
     *
     * @throws ApiException If there is an error during the API call.
     */
    protected ApiResponse call(@NonNull ApiRequest apiRequest) {
        Objects.requireNonNull(apiRequest, "apiRequest must not be null"); // Defensive programming
        log.info("Calling API with method: {} and path: {}", apiRequest.getMethod(), apiRequest.getPath());
        log.debug("ApiRequest details: {}", apiRequest);

        try {
            WebClient.RequestBodySpec requestBodySpec = configureRequest(apiRequest);
            configureHeaders(apiRequest, requestBodySpec);
            configureBody(apiRequest, requestBodySpec);
            log.debug("requestBodySpec properties : {}", requestBodySpec.getClass());

            ApiResponse apiResponse = requestBodySpec.exchangeToMono(this::handleResponse).timeout(DEFAULT_TIMEOUT)
                                                     .onErrorMap(this::mapException).block();
            log.debug("Received apiResponse: {}", apiResponse);
            return apiResponse;

        } catch (ApiException e) {
            log.error("Exception during  API call", e);
            throw e;
        } catch (Exception e) {
            log.error("Exception during  API call", e);
            throw mapException(e); // Re-wrap exception
        }
    }


    public <T> Flux<ServerSentEvent<T>> consumeStream(ApiRequest apiRequest,
                                                      Consumer<ServerSentEvent<T>> chunkReceiveEvent,
                                                      Consumer<Throwable> errorConsumer, Runnable completionEvent,
                                                      ParameterizedTypeReference<ServerSentEvent<T>> parameterizedTypeReference) {
        log.info("Starting to consume SSE stream from: {}", apiRequest.getPath());
        try {
            MediaType acceptMediaType = Optional.ofNullable(apiRequest.getAcceptMediaType())
                                                .orElse(MediaType.TEXT_EVENT_STREAM);

            WebClient.RequestBodySpec requestBodySpec = configureRequest(apiRequest);
            requestBodySpec.accept(acceptMediaType);
            configureHeaders(apiRequest, requestBodySpec);
            configureBody(apiRequest, requestBodySpec);

            return requestBodySpec.retrieve().bodyToFlux(parameterizedTypeReference)
                                  .onBackpressureBuffer(100, BufferOverflowStrategy.DROP_OLDEST)
                                  .doOnNext(chunkReceiveEvent).doOnError(errorConsumer).doOnComplete(completionEvent);
        } catch (Exception e) {
            log.error("Exception during streaming API call");
            Throwable mappedException = mapException(e);
            errorConsumer.accept(mappedException);
            return Flux.error(mappedException);
        }
    }

    /**
     * Maps exceptions to specific custom exceptions based on the type of exception and, for {@link
     * WebClientResponseException}, the HTTP status code.
     *
     * @param error The throwable error.
     *
     * @return A specific {@link RuntimeException} representing the error.
     */
    private RuntimeException mapException(Throwable error) {
        log.warn("Mapping exception: {}", error.getMessage(), error);
        if (error instanceof WebClientResponseException webClientError) {
            String responseBody = webClientError.getResponseBodyAsString();
            int statusCode = webClientError.getStatusCode().value();
            log.warn("exception : {} with body {} ", statusCode, responseBody);
            RuntimeException newException = createException(responseBody, statusCode);
            log.debug("Map exception: {}", newException);
            return newException;

        } else if (error instanceof WebClientRequestException || error instanceof ConnectException ||
                   error instanceof java.net.UnknownHostException) {
            ServiceUnavailableException exception = new ServiceUnavailableException(
                    "Failed to connect to external service: " + error.getMessage());
            log.error("an exception to do the service is unavailable ", exception);
            return exception;

        } else if (error instanceof java.util.concurrent.TimeoutException) {
            GatewayTimeoutException exception = new GatewayTimeoutException("Request timed out: " + error.getMessage());
            log.error("an time out exception was found ", exception);
            return exception;

        } else if (error instanceof WebClientException) {
            ApiException exception = new ApiException("Unexpected WebClient error: " + error.getMessage(),
                                                      HttpStatus.INTERNAL_SERVER_ERROR.value());
            log.error("An error occurred on web client", exception);
            return exception;

        } else {
            ApiException exception = new ApiException("Internal API client error: " + error.getMessage(),
                                                      HttpStatus.INTERNAL_SERVER_ERROR.value());
            log.error("An error occurred on API client", exception);
            return exception;
        }
    }

    /**
     * Configures the WebClient request by setting the HTTP method and URI. Query parameters and path
     * variables are added to the URI.
     *
     * @param apiRequest The API request containing the configuration.
     *
     * @return A {@link WebClient.RequestBodySpec} configured with the method and URI.
     */
    private WebClient.RequestBodySpec configureRequest(ApiRequest apiRequest) {
        log.debug("configure Request {} with path : {}", apiRequest.getMethod(), apiRequest.getPath());
        WebClient.RequestBodySpec requestBodySpec = webClient.method(apiRequest.getMethod()).uri(uriBuilder -> {
            uriBuilder.path(apiRequest.getPath());

            Optional.ofNullable(apiRequest.getQueryParams())
                    .ifPresent(params -> params.forEach(uriBuilder::queryParam));

            return uriBuilder.build(Optional.ofNullable(apiRequest.getPathVariables()).orElse(Collections.emptyMap()));
        });
        log.trace("parameters for configureRequest: {}", apiRequest.getPath());

        return requestBodySpec;
    }

    /**
     * Configures the headers for the WebClient request. Authentication headers, custom headers from
     * {@link HeaderConfig}, and headers from the {@link ApiRequest} are applied.
     *
     * @param apiRequest      The API request containing the header configuration.
     * @param requestBodySpec The request body specification to configure.
     */
    private void configureHeaders(ApiRequest apiRequest, WebClient.RequestBodySpec requestBodySpec) {
        log.debug("configure Headers: {}", apiRequest.getHeaders());
        // Apply authentication headers
        authentication.applyAuthentication(apiRequest.getHeaders());
        log.debug("Authentication applied for the header, size: {}", apiRequest.getHeaders().size());

        // Apply custom headers from HeaderConfig
        if (headerConfig != null && headerConfig.getHeaders() != null) {
            headerConfig.getHeaders().forEach(header -> {
                requestBodySpec.header(header.getName(), header.getValue());
            });
        }

        // Apply headers from the ApiRequest
        Optional.ofNullable(apiRequest.getHeaders()).ifPresent(headers -> {
            headers.forEach(requestBodySpec::header);
            log.trace("requestBodySpec headers value: {}", headers);
        });

        // Set the accept media type
        Optional.ofNullable(apiRequest.getAcceptMediaType()).ifPresent(requestBodySpec::accept);
    }

    /**
     * Configures the request body for the WebClient request. If a body is present in the {@link
     * ApiRequest}, it is added to the request with the specified content type.
     *
     * @param apiRequest      The API request containing the body and content type.
     * @param requestBodySpec The request body specification to configure.
     */
    private void configureBody(ApiRequest apiRequest, WebClient.RequestBodySpec requestBodySpec) {
        log.debug("Configuring request body, ApiRequest has a body: {} and apiRequest has a contentType: {}",
                  apiRequest.getBody() != null, apiRequest.getContentType());
        if (apiRequest.getBody() == null) {
            log.debug("There is no body here");
            return;
        }

        // Determine content type
        MediaType contentType = Optional.ofNullable(apiRequest.getContentType()).orElse(MediaType.APPLICATION_JSON);
        requestBodySpec.contentType(contentType);
        log.trace("ContentType = {} ", contentType);
        // Set the body
        try {
            requestBodySpec.body(BodyInserters.fromValue(apiRequest.getBody()));
        } catch (Exception e) {
            log.error("Invalid request body {}", e.getMessage());
            throw new BadRequestException("Invalid request body: " + e.getMessage());
        }
    }

    /**
     * Handles the {@link ClientResponse} from the WebClient. If the response is successful (2xx
     * status code), the response body is extracted and used to build an {@link ApiResponse}. If the
     * response is an error (non-2xx status code), an appropriate exception is created.
     *
     * @param response The client response.
     *
     * @return A {@link Mono} emitting the {@link ApiResponse}.
     */
    private Mono<ApiResponse> handleResponse(ClientResponse response) {
        log.debug("Handling response with status code: {}", response.statusCode());
        Instant timestamp = Instant.now();
        HttpHeaders headers = response.headers().asHttpHeaders();
        MediaType acceptType = headers.getContentType();
        int statusCode = response.statusCode().value();

        if (response.statusCode().is2xxSuccessful()) {
            log.debug("Response was successful, statusCode {}", statusCode);
            return handleSuccessResponse(response, headers, acceptType, statusCode, timestamp);
        } else {
            log.warn("Response was NOT successful, statusCode {}", statusCode);
            return handleErrorResponse(response, statusCode);
        }
    }

    /**
     * Handles a successful (2xx) {@link ClientResponse}. Extracts the response body and builds an
     * {@link ApiResponse}.
     *
     * @param response   The client response.
     * @param headers    The HTTP headers from the response.
     * @param acceptType The accept media type from the response.
     * @param statusCode The HTTP status code.
     * @param timestamp  The timestamp of the response.
     *
     * @return A {@link Mono} emitting the {@link ApiResponse}.
     */
    private Mono<ApiResponse> handleSuccessResponse(ClientResponse response, HttpHeaders headers, MediaType acceptType,
                                                    int statusCode, Instant timestamp) {
        log.debug("Handling successful response, status code: {}", statusCode);

        return response.bodyToMono(byte[].class).map(data -> {
            ApiResponse apiResponse = ApiResponse.builder().data(data).acceptType(acceptType).headers(headers)
                                                 .statusCode(statusCode).timestamp(timestamp).error(null).build();
            log.debug("Api Response  was success  =  {} ", apiResponse.getStatusCode());
            return apiResponse;
        }).onErrorMap(error -> {
            log.error("Error processing successful response body", error);
            return new ApiException("Error processing response: " + error.getMessage(), statusCode);
        });
    }

    /**
     * Handles an error (non-2xx) {@link ClientResponse}. Extracts the error message from the response
     * body and creates an appropriate exception.
     *
     * @param response   The client response.
     * @param statusCode The HTTP status code.
     *
     * @return A {@link Mono} emitting an error.
     */
    private Mono<ApiResponse> handleErrorResponse(ClientResponse response, int statusCode) {
        log.warn("Handling error response, status code: {}", statusCode);
        return response.bodyToMono(String.class).flatMap(body -> {
            log.warn("Body has been parsed and it's printing an error: {}", body);
            return Mono.error(createException(body, statusCode));
        });
    }

    /**
     * Creates an appropriate {@link ApiException} based on the provided HTTP status code and error
     * message.
     *
     * @param body       The error message from the response body.
     * @param statusCode The HTTP status code.
     *
     * @return An {@link ApiException} representing the error.
     */
    private ApiException createException(String body, int statusCode) {
        log.debug("Creating exception for status code: {}, body: {}", statusCode, body);
        ApiException exception = switch (statusCode) {
            case 400 -> new BadRequestException(body);
            case 401 -> new UnauthorizedException(body);
            case 403 -> new ForbiddenException(body);
            case 404 -> new NotFoundException(body);
            case 409 -> new ConflictException(body);
            case 429 -> new TooManyRequestsException(body);
            case 500 -> new InternalServerException(body);
            case 502 -> new BadGatewayException(body);
            case 503 -> new ServiceUnavailableException(body);
            case 504 -> new GatewayTimeoutException(body);
            default -> new ApiException(body, statusCode);
        };
        log.warn("Api request failing {}", exception.getMessage());
        return exception;
    }
}