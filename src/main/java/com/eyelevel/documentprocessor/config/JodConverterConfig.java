package com.eyelevel.documentprocessor.config;

import org.jodconverter.local.office.LocalOfficeManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configures and manages the lifecycle of a local LibreOffice instance for document conversions.
 * This class is responsible for creating a managed bean that can be used by the application
 * to convert documents between various formats (e.g., DOCX to PDF).
 */
@Configuration
public class JodConverterConfig {

    /**
     * Creates and initializes a {@link LocalOfficeManager} bean. The OfficeManager controls
     * a running LibreOffice process, managing its startup, shutdown, and task queue.
     * The configuration for the office process is externalized to application properties.
     *
     * @param officeHome           The file system path to the LibreOffice installation directory.
     * @param portNumbers          An array of network ports on which the LibreOffice process will listen.
     *                             Spring Boot automatically converts a comma-separated string from properties
     *                             into this array.
     * @param taskExecutionTimeout The maximum time in milliseconds a single conversion task is allowed to run
     *                             before it is terminated.
     *
     * @return A fully configured and managed {@link LocalOfficeManager} instance.
     */
    @Bean(initMethod = "start", destroyMethod = "stop")
    public LocalOfficeManager localOfficeManager(@Value("${app.jodconverter.office.home}") String officeHome,
                                                 @Value("${app.jodconverter.office.port-numbers}") int[] portNumbers,
                                                 @Value("${app.jodconverter.office.task-execution-timeout}")
                                                 long taskExecutionTimeout) {
        return LocalOfficeManager.builder().officeHome(officeHome).portNumbers(portNumbers)
                                 .taskExecutionTimeout(taskExecutionTimeout).build();
    }
}