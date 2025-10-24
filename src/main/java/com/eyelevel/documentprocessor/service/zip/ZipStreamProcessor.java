package com.eyelevel.documentprocessor.service.zip;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FilenameUtils;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Handles the low-level, single-threaded processing of a ZIP file stream.
 * This class is responsible for sequentially reading a ZIP archive, extracting each valid entry
 * into a temporary file, calculating its SHA-256 hash, and then passing the details to a
 * consumer for further (potentially concurrent) processing.
 */
@Slf4j
@Component
public class ZipStreamProcessor {

    //<editor-fold desc="Constants">
    /**
     * A set of common, ignorable file and directory names often found in ZIP archives,
     * such as macOS resource forks and Windows thumbnail caches.
     */
    private static final Set<String> IGNORED_ENTRIES = Set.of("__MACOSX", ".DS_Store", "Thumbs.db");
    //</editor-fold>

    //<editor-fold desc="Public API">

    /**
     * Processes a ZIP archive from an input stream in a single thread.
     * <p>
     * This method sequentially reads each entry from the {@link ZipInputStream}. For each valid entry, it streams
     * the content into a temporary file on disk while simultaneously calculating its SHA-256 hash. This approach
     * avoids holding the entire file content in memory. Once an entry is fully written and its metadata is captured,
     * it is passed to the provided {@code workItemConsumer} to be handed off for parallel processing.
     *
     * @param zipStream        The input stream of the ZIP archive to be processed.
     * @param tempDir          The directory where temporary files for each ZIP entry will be stored.
     * @param workItemConsumer A consumer that accepts a {@link ZipEntryWorkItem} for each valid entry.
     * @throws IOException if an I/O error occurs while reading the ZIP stream or writing to temporary files.
     */
    public void processStream(InputStream zipStream, Path tempDir, Consumer<ZipEntryWorkItem> workItemConsumer) throws IOException {
        // Use a try-with-resources block to ensure the ZipInputStream is properly closed.
        try (ZipInputStream zis = new ZipInputStream(zipStream)) {
            ZipEntry currentEntry;
            // Sequentially read each entry from the ZIP stream until there are no more.
            while ((currentEntry = zis.getNextEntry()) != null) {
                try {
                    final String normalizedPath = currentEntry.getName().replace('\\', '/');
                    if (shouldSkipEntry(currentEntry, normalizedPath)) {
                        continue; // Advance the stream to the next entry.
                    }
                    processSingleEntry(zis, tempDir, normalizedPath, workItemConsumer);
                } finally {
                    // Ensure the current entry is closed before moving to the next one.
                    zis.closeEntry();
                }
            }
        }
    }
    //</editor-fold>

    //<editor-fold desc="Private Helper Methods">

    /**
     * Processes a single, valid entry from the ZipInputStream.
     */
    private void processSingleEntry(ZipInputStream zis, Path tempDir, String normalizedPath, Consumer<ZipEntryWorkItem> workItemConsumer) throws IOException {
        Path tempFile = null;
        try {
            tempFile = Files.createTempFile(tempDir, "zip-entry-", ".tmp");
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            long fileSize;

            // Stream the entry's content into the temp file. The DigestOutputStream calculates the hash
            // as the data is being written, avoiding a second read pass.
            try (OutputStream fileOut = Files.newOutputStream(tempFile);
                 DigestOutputStream digestOut = new DigestOutputStream(fileOut, sha256)) {
                fileSize = zis.transferTo(digestOut);
            }

            // Only process entries that have content.
            if (fileSize > 0) {
                String fileHash = Hex.encodeHexString(sha256.digest());
                ZipEntryWorkItem workItem = new ZipEntryWorkItem(normalizedPath, tempFile, fileHash, fileSize);
                workItemConsumer.accept(workItem);
            } else {
                // If the file is empty, delete the temporary file immediately to save space.
                Files.delete(tempFile);
            }
        } catch (NoSuchAlgorithmException e) {
            // This exception is highly unlikely as SHA-256 is a standard algorithm required by the JVM.
            // If it occurs, it's a fatal environment error, so we wrap it in a RuntimeException.
            throw new RuntimeException("SHA-26 algorithm not available.", e);
        } catch (IOException e) {
            cleanupTempFileOnError(tempFile, e);
            log.error("Failed to stream ZIP entry '{}' to temporary file.", normalizedPath, e);
            throw e; // Re-throw the exception to indicate failure of the overall process.
        }
    }

    /**
     * Determines whether a given ZIP entry should be skipped based on its name and type.
     *
     * @param entry          The {@link ZipEntry} to evaluate.
     * @param normalizedPath The normalized path of the entry.
     * @return {@code true} if the entry should be skipped, {@code false} otherwise.
     */
    private boolean shouldSkipEntry(final ZipEntry entry, final String normalizedPath) {
        // Skip directories and entries that end with a slash.
        if (entry.isDirectory() || normalizedPath.endsWith("/")) {
            return true;
        }

        final String fileName = FilenameUtils.getName(normalizedPath);
        final String rootDir = normalizedPath.contains("/")
                               ? normalizedPath.substring(0, normalizedPath.indexOf('/'))
                               : ""; // An entry in the root has no root directory name.

        // Skip if the file is a known system file or an AppleDouble resource fork (starts with '._').
        return IGNORED_ENTRIES.contains(fileName) || IGNORED_ENTRIES.contains(rootDir) || fileName.startsWith("._");
    }

    /**
     * Attempts to clean up a temporary file after an I/O error has occurred.
     */
    private void cleanupTempFileOnError(Path tempFile, IOException originalException) {
        if (tempFile != null) {
            try {
                Files.deleteIfExists(tempFile);
            } catch (IOException cleanupEx) {
                // Suppress the cleanup exception to prioritize the original I/O error.
                originalException.addSuppressed(cleanupEx);
            }
        }
    }
    //</editor-fold>

    //<editor-fold desc="Public Records">
    /**
     * A record to encapsulate the metadata of a processed ZIP entry.
     * This object serves as a data transfer object, containing all the necessary information
     * for a worker thread to process a single file extracted from the ZIP archive.
     *
     * @param normalizedPath The cleaned, forward-slash-separated path of the entry within the ZIP archive.
     * @param tempFilePath   The {@link Path} to the temporary file on disk where the entry's content is stored.
     * @param sha256Hash     The SHA-256 hash of the file content, represented as a hexadecimal string.
     * @param fileSize       The size of the file in bytes.
     */
    public record ZipEntryWorkItem(String normalizedPath, Path tempFilePath, String sha256Hash, long fileSize) {
    }
    //</editor-fold>
}