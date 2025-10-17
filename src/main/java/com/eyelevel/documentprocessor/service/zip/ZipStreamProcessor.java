package com.eyelevel.documentprocessor.service.zip;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * A reusable, stateless component for processing ZIP streams. It efficiently iterates through
 * ZIP entries one-by-one to maintain a low memory footprint, handles nested ZIP archives recursively,
 * and invokes a functional callback for each valid file.
 */
@Slf4j
@Component
public class ZipStreamProcessor {

    private static final Set<String> IGNORED_FILES = Set.of("__MACOSX", ".DS_Store", "Thumbs.db");
    private static final int COPY_BUFFER = 8192;

    /**
     * Processes a ZIP archive from an InputStream, applying the provided consumer to each valid file.
     * This method is the primary entry point for the processor.
     *
     * @param zipStream    The input stream of the ZIP file to process.
     * @param fileConsumer A BiConsumer that accepts the normalized entry name (path) and a Path
     *                     to a temporary file containing the entry's content. The temporary file
     *                     is guaranteed to be deleted after the consumer completes.
     * @throws IOException if a critical I/O error occurs during stream processing.
     */
    public void process(InputStream zipStream, BiConsumer<String, Path> fileConsumer) throws IOException {
        processEntries(new ZipInputStream(zipStream), fileConsumer);
    }

    /**
     * Recursively iterates through ZIP entries, streaming each to a temporary file and invoking the consumer.
     */
    private void processEntries(ZipInputStream zis, BiConsumer<String, Path> fileConsumer) throws IOException {
        ZipEntry entry;
        while ((entry = zis.getNextEntry()) != null) {
            String normalizedPath = entry.getName().replace('\\', '/');

            if (shouldSkipEntry(entry, normalizedPath)) {
                zis.closeEntry();
                continue;
            }

            Path tempFile = streamEntryToTempFile(zis, normalizedPath);
            try {
                if (Files.size(tempFile) == 0) {
                    log.debug("Skipping empty file entry: {}", normalizedPath);
                    continue;
                }

                if (FilenameUtils.isExtension(normalizedPath.toLowerCase(), "zip")) {
                    log.info("Found nested ZIP archive, beginning recursive processing: {}", normalizedPath);
                    try (InputStream nestedStream = Files.newInputStream(tempFile)) {
                        process(nestedStream, fileConsumer);
                    }
                    log.info("Finished processing nested ZIP archive: {}", normalizedPath);
                } else {
                    fileConsumer.accept(normalizedPath, tempFile);
                }
            } finally {
                Files.deleteIfExists(tempFile);
                zis.closeEntry();
            }
        }
    }

    /**
     * Determines if a given ZIP entry should be skipped based on its type or name.
     */
    private boolean shouldSkipEntry(ZipEntry entry, String normalizedPath) {
        if (entry.isDirectory() || normalizedPath.endsWith("/")) {
            return true;
        }

        String fileName = FilenameUtils.getName(normalizedPath);
        String rootDir = normalizedPath.contains("/") ? normalizedPath.substring(0, normalizedPath.indexOf('/')) : normalizedPath;

        if (IGNORED_FILES.contains(fileName) || IGNORED_FILES.contains(rootDir)) {
            log.debug("Ignoring system or metadata entry: {}", normalizedPath);
            return true;
        }
        return false;
    }

    /**
     * Safely streams a ZIP entry's content to a new temporary file.
     */
    private Path streamEntryToTempFile(ZipInputStream zis, String entryName) throws IOException {
        String suffix = entryName.contains(".") ? entryName.substring(entryName.lastIndexOf('.')) : ".bin";
        Path tempFile = Files.createTempFile("zip-entry-", suffix);

        try (var out = Files.newOutputStream(tempFile)) {
            byte[] buf = new byte[COPY_BUFFER];
            int read;
            while ((read = zis.read(buf)) != -1) {
                out.write(buf, 0, read);
            }
        } catch (IOException e) {
            Files.deleteIfExists(tempFile);
            throw e;
        }
        return tempFile;
    }
}