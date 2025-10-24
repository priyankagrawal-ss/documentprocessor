package com.eyelevel.documentprocessor.common.processexec;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Component
@Slf4j
public class ProcessExecutor {

    /**
     * A safe limit for the amount of stdout/stderr to capture in memory.
     * 16 KB is enough to capture most error messages without risking OutOfMemoryError.
     */
    private static final int MAX_CAPTURE_BYTES = 16 * 1024;

    /**
     * Executes a command-line process with a timeout and memory-safe stream handling.
     *
     * @param command The command and its arguments to execute.
     * @param contextInfo A string for logging context (e.g., FileMaster ID).
     * @param timeoutMinutes The maximum time to wait for the process to complete.
     * @param processName A descriptive name for the process (e.g., "Ghostscript").
     * @return A ProcessResult containing the exit code and a truncated portion of stdout and stderr.
     * @throws IOException if the process times out or an I/O error occurs.
     * @throws InterruptedException if the waiting thread is interrupted.
     */
    public ProcessResult execute(List<String> command, String contextInfo, long timeoutMinutes, String processName)
    throws IOException, InterruptedException {

        Process process = new ProcessBuilder(command).start();
        StringBuilder stdoutCapture = new StringBuilder(MAX_CAPTURE_BYTES);
        StringBuilder stderrCapture = new StringBuilder(MAX_CAPTURE_BYTES);

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            // Create stream consumers that will capture output up to a safe limit.
            StreamConsumer stdoutConsumer = new StreamConsumer(process.getInputStream(), stdoutCapture::append, null);
            StreamConsumer stderrConsumer = new StreamConsumer(process.getErrorStream(), stderrCapture::append,
                                                               line -> log.warn("[{}] [{}-stderr] {}", contextInfo, processName, line)
            );

            executor.submit(stdoutConsumer);
            executor.submit(stderrConsumer);

            if (!process.waitFor(timeoutMinutes, TimeUnit.MINUTES)) {
                process.destroyForcibly();
                throw new IOException(processName + " process timed out after " + timeoutMinutes + " minutes.");
            }
        }

        return new ProcessResult(process.exitValue(), stdoutCapture.toString().trim(), stderrCapture.toString().trim());
    }

    /**
     * A Runnable that consumes an InputStream, captures its content up to a limit,
     * and optionally logs each line. This prevents both deadlocks and OutOfMemoryErrors.
     */
    private static class StreamConsumer implements Runnable {
        private final InputStream inputStream;
        private final Consumer<String> captureConsumer;
        private final Consumer<String> lineLogger;
        private int bytesCaptured = 0;

        public StreamConsumer(InputStream inputStream, Consumer<String> captureConsumer, Consumer<String> lineLogger) {
            this.inputStream = inputStream;
            this.captureConsumer = captureConsumer;
            this.lineLogger = lineLogger;
        }

        @Override
        public void run() {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (lineLogger != null) {
                        lineLogger.accept(line);
                    }
                    // Only append to the capture buffer if we are within the size limit.
                    if (bytesCaptured < MAX_CAPTURE_BYTES) {
                        String lineWithNewline = line + "\n";
                        captureConsumer.accept(lineWithNewline);
                        bytesCaptured += lineWithNewline.getBytes().length;
                    }
                }
            } catch (IOException e) {
                log.error("Error reading process stream.", e);
            }
        }
    }

    /**
     * A record to hold the result of an external process execution.
     *
     * @param exitCode The exit code of the process. 0 typically means success.
     * @param stdout   The captured standard output (truncated to a safe limit).
     * @param stderr   The captured standard error output (truncated to a safe limit).
     */
    public record ProcessResult(int exitCode, String stdout, String stderr) {
    }
}