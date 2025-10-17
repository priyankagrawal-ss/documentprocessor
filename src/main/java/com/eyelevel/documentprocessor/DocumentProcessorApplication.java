package com.eyelevel.documentprocessor;

import com.eyelevel.documentprocessor.config.DocumentProcessingConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * The main entry point for the Document Processor Spring Boot application.
 * <p>
 * This class bootstraps the application context and enables key Spring features:
 * <ul>
 *     <li>{@link SpringBootApplication}: A composite annotation that enables auto-configuration,
 *     component scanning, and property support.</li>
 *     <li>{@link EnableConfigurationProperties}: Binds custom application properties (prefixed with "app.processing")
 *     to the {@link DocumentProcessingConfig} class.</li>
 *     <li>{@link EnableScheduling}: Activates Spring's scheduled task execution capabilities for background jobs
 *     like status syncing and cleanup.</li>
 *     <li>{@link EnableAsync}: Enables Spring's asynchronous method execution capabilities.</li>
 *     <li>{@link EnableJpaRepositories}: Configures the base package for scanning Spring Data JPA repositories.</li>
 * </ul>
 */
@Slf4j
@EnableAsync
@EnableScheduling
@SpringBootApplication
@EnableJpaRepositories(basePackages = "com.eyelevel.documentprocessor.repository")
@EnableConfigurationProperties(value = DocumentProcessingConfig.class)
@EnableRetry
public class DocumentProcessorApplication {

    /**
     * The main method which serves as the entry point for the Java application.
     * It delegates to Spring Boot's {@link SpringApplication} class to launch the application,
     * and logs key environment information upon startup.
     *
     * @param args Command-line arguments passed to the application.
     */
    public static void main(final String[] args) {
        log.info("🚀 Starting DocumentProcessorApplication...");

        final ConfigurableApplicationContext context = SpringApplication.run(DocumentProcessorApplication.class, args);
        final Environment env = context.getEnvironment();

        log.info("------------------------------------------------------------------");
        log.info("Application '{}' is now running!", env.getProperty("spring.application.name", "DocumentProcessor"));
        log.info("Access URLs:");
        log.info("  - Local:      http://localhost:{}", env.getProperty("server.port", "8080"));
        log.info("  - Profile(s): {}", String.join(", ", env.getActiveProfiles().length > 0
                ? env.getActiveProfiles()
                : new String[]{"default"}));
        log.info("------------------------------------------------------------------");
    }
}