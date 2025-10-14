package com.eyelevel.documentprocessor.common.apiclient.gx.config;

import com.eyelevel.documentprocessor.common.apiclient.authentication.Authentication;
import com.eyelevel.documentprocessor.common.apiclient.authentication.impl.APIKeyAuthentication;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
@Slf4j
public class GXApiClientConfiguration {
    @Value("${app.gx-client.baseurl}")
    private String baseUrl;

    @Value("${app.gx-client.auth-key-name}")
    private String headerName;

    @Value("${app.gx-client.auth-key-value}")
    private String headerValue;

    @Bean("gxWebClient")
    public WebClient groundXWebClient() {
        log.info("Configuring GroundX WebClient with base URL: {}", baseUrl);
        WebClient webClient = WebClient.builder().baseUrl(baseUrl).build();
        log.debug("Created GroundX WebClient: {}", webClient);
        return webClient;
    }

    @Bean("gxAuthentication")
    public Authentication groundXAuthentication() {
        log.info("Configuring GroundX Authentication with headerName: {}, headerValue: {}", headerName, headerValue);
        Authentication authentication = new APIKeyAuthentication(headerName, headerValue);
        log.debug("Created APIKeyAuthentication instance: {}", authentication.getClass());
        return authentication;
    }
}
