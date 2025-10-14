package com.eyelevel.documentprocessor.common.apiclient.gx.config;

import com.eyelevel.documentprocessor.common.apiclient.authentication.Authentication;
import com.eyelevel.documentprocessor.common.apiclient.authentication.impl.APIKeyAuthentication;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * Configures the necessary beans for the GroundX (GX) API client, including the
 * {@link WebClient} for communication and the {@link Authentication} mechanism.
 */
@Slf4j
@Configuration
public class GXApiClientConfiguration {

    @Value("${app.gx-client.baseurl}")
    private String baseUrl;

    @Value("${app.gx-client.auth-key-name}")
    private String headerName;

    @Value("${app.gx-client.auth-key-value}")
    private String headerValue;

    /**
     * Creates and configures the {@link WebClient} instance for connecting to the GroundX API.
     *
     * @return A configured {@link WebClient} bean named "gxWebClient".
     */
    @Bean("gxWebClient")
    public WebClient groundXWebClient() {
        log.info("Initializing GroundX WebClient with base URL: {}", baseUrl);
        return WebClient.builder()
                .baseUrl(baseUrl)
                .build();
    }

    /**
     * Creates the {@link Authentication} bean for the GroundX API using API key authentication.
     *
     * @return An {@link APIKeyAuthentication} instance configured with credentials from application properties.
     */
    @Bean("gxAuthentication")
    public Authentication groundXAuthentication() {
        log.info("Initializing GroundX authentication with header name: '{}'", headerName);
        if (headerValue == null || headerValue.isBlank()) {
            log.warn("GroundX API key is not configured. API calls may fail authentication.");
        }
        return new APIKeyAuthentication(headerName, headerValue);
    }
}