package com.eyelevel.documentprocessor.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.Optional;

@Configuration
@Profile("!prod")
@RequiredArgsConstructor
public class OpenApiConfig {


    private final Optional<BuildProperties> buildProperties;

    @Bean
    public OpenAPI customOpenAPI() {
        String version = buildProperties.map(BuildProperties::getVersion).orElse("<NOT_FOUND>");
        String appName = buildProperties.map(BuildProperties::getName).orElse("Document Processor API");

        return new OpenAPI()
                .info(new Info().title(appName)
                        .version(version)
                        .description("""
                                This API provides a comprehensive workflow for processing documents.
                                It supports direct and multipart file uploads, queues files for processing,
                                handles various document formats (PDFs, Office documents, emails),
                                and tracks the status of each processing job.
                                
                                Key features include:
                                * **Asynchronous Processing:** Uploaded files are queued and processed asynchronously.
                                * **Versatile Handlers:** Supports PDF optimization/splitting, Office document conversion, and email (.msg) attachment extraction.
                                * **Job Lifecycle Management:** Full control over job initiation, termination, and retries.
                                * **Status Monitoring:** Endpoints to view document status and aggregated metrics.
                                
                                **Note:** Administrative endpoints, such as terminating jobs, are marked and should be used with caution.
                                """)
                        .contact(new Contact()
                                .name("EyeLevel.ai Support")
                                .url("https://www.eyelevel.ai")));
    }
}