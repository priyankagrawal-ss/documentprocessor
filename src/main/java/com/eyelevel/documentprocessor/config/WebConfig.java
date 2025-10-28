package com.eyelevel.documentprocessor.config;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.Arrays;

/**
 * Centralized configuration for web-related beans and settings, including CORS.
 */
@Slf4j
@Configuration
public class WebConfig {

    @Value("${app.cors.allowed-origins}")
    private String[] allowedOrigins;

    /**
     * Defines a global CORS configuration for the entire application.
     * This bean is automatically picked up by Spring MVC.
     *
     * @return a WebMvcConfigurer with the defined CORS mapping.
     */
    @Bean
    public WebMvcConfigurer corsConfigurer() {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(@NonNull CorsRegistry registry) {
                log.info("CORS allowed origins: {}", Arrays.toString(allowedOrigins));
                registry.addMapping("/**")
                        .allowedOrigins(allowedOrigins)
                        .allowedMethods("*")
                        .allowedHeaders("*")
                        .allowCredentials(true)
                        .maxAge(3600); // Cache pre-flight response for 1 hour
            }
        };
    }


}
