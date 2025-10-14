package com.eyelevel.documentprocessor;

import com.eyelevel.documentprocessor.config.DocumentProcessingConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * The main entry point for the Document Processor Spring Boot application.
 * <p>
 * This class is responsible for bootstrapping the application context and enabling key features:
 * <ul>
 *     <li>{@code @SpringBootApplication}: Standard Spring Boot configuration, component scanning, and auto-configuration.</li>
 *     <li>{@code @EnableConfigurationProperties}: Binds custom application properties to the {@link DocumentProcessingConfig} class.</li>
 *     <li>{@code @EnableScheduling}: Activates Spring's scheduled task execution capabilities, used for periodic jobs like status syncing and cleanup.</li>
 *     <li>{@code @EnableAsync}: Enables Spring's asynchronous method execution capabilities.</li>
 *     <li>{@code @EnableJpaRepositories}: Explicitly configures the base package for scanning Spring Data JPA repositories.</li>
 * </ul>
 */
@SpringBootApplication
@EnableConfigurationProperties(value = DocumentProcessingConfig.class)
@EnableScheduling
@EnableAsync
@EnableJpaRepositories(basePackages = "com.eyelevel.documentprocessor.repository")
@Slf4j
public class DocumentProcessorApplication {

    /**
     * The main method which serves as the entry point for the Java application.
     * It delegates to Spring Boot's {@link SpringApplication} class to launch the application.
     *
     * @param args Command-line arguments passed to the application.
     */
    public static void main(String[] args) {
        log.info("Starting DocumentProcessorApplication...");
        SpringApplication.run(DocumentProcessorApplication.class, args);
        log.info("DocumentProcessorApplication has started successfully.");
    }
}