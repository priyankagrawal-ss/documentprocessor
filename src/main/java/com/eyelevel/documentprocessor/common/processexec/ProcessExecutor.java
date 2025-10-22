package com.eyelevel.documentprocessor.common.processexec;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class ProcessExecutor {

    public ProcessResult execute(List<String> command, String contextInfo, long timeoutMinutes, String processName)
    throws IOException, InterruptedException {
        Process process = new ProcessBuilder(command).start();
        StringBuilder stdout = new StringBuilder();
        StringBuilder stderr = new StringBuilder();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executor.submit(() -> new BufferedReader(new InputStreamReader(process.getInputStream())).lines().forEach(
                    stdout::append));
            executor.submit(
                    () -> new BufferedReader(new InputStreamReader(process.getErrorStream())).lines().forEach(line -> {
                        log.warn("[{}] [{}-stderr] {}", contextInfo, processName, line);
                        stderr.append(line).append("\n");
                    }));
            if (!process.waitFor(timeoutMinutes, TimeUnit.MINUTES)) {
                process.destroyForcibly();
                throw new IOException(processName + " process timed out after " + timeoutMinutes + " minutes.");
            }
        }
        return new ProcessResult(process.exitValue(), stdout.toString().trim(), stderr.toString().trim());
    }

    public record ProcessResult(int exitCode, String stdout, String stderr) {
    }
}