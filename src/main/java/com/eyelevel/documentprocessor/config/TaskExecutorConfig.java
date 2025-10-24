package com.eyelevel.documentprocessor.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Configures a central, managed thread pool for handling asynchronous application tasks,
 * such as the completion callbacks for S3 uploads. This provides better resource management
 * and is configurable via application.yaml.
 */
@Configuration
public class TaskExecutorConfig {

    /**
     * Creates the primary thread pool for async tasks. The properties for this pool are
     * configured in application.yaml under the `spring.task.execution.pool` prefix.
     *
     * @return A configured AsyncTaskExecutor bean.
     */
    @Bean("applicationTaskExecutor")
    public AsyncTaskExecutor applicationTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // Configuration for core size, max size, and queue capacity will be automatically
        // applied by Spring Boot from the `spring.task.execution.pool.*` properties.
        executor.setThreadNamePrefix("app-task-");
        executor.initialize();
        return executor;
    }
}