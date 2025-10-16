package com.eyelevel.documentprocessor.config;

import org.jodconverter.local.office.LocalOfficeManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JodConverterConfig {

    @Bean(initMethod = "start", destroyMethod = "stop")
    public LocalOfficeManager localOfficeManager() {
        // You could pull these values from DocumentProcessingConfig if you want them to be dynamic
        return LocalOfficeManager.builder()
                .officeHome("/usr/lib/libreoffice") // Or use a value from your properties
                .portNumbers(8100)
                .taskExecutionTimeout(120000L) // 2 minutes in milliseconds
                .build();
    }
}