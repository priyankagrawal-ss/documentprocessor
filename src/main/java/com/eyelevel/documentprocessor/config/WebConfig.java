package com.eyelevel.documentprocessor.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Centralized configuration for web-related beans and settings, including CORS.
 */
@Configuration
public class WebConfig {

    // Inject the allowed origins from application.yaml.
    // This allows you to easily change the URL for different environments (dev, staging, prod).
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
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/api/**") // Apply CORS policy to all endpoints under /api
                        .allowedOrigins(allowedOrigins)
                        .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH") // Standard REST methods
                        .allowedHeaders("*") // Allow all headers
                        .allowCredentials(true) // Allow cookies and authentication headers
                        .maxAge(3600); // Cache pre-flight response for 1 hour
            }
        };
    }
}