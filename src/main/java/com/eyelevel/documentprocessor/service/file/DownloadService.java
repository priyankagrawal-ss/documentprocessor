package com.eyelevel.documentprocessor.service.file;

import com.eyelevel.documentprocessor.dto.presign.download.PresignedDownloadResponse;
import com.eyelevel.documentprocessor.dto.presign.request.DownloadFileRequest;
import com.eyelevel.documentprocessor.exception.DocumentProcessingException;
import com.eyelevel.documentprocessor.exception.apiclient.NotFoundException;
import com.eyelevel.documentprocessor.repository.FileMasterRepository;
import com.eyelevel.documentprocessor.repository.GxMasterRepository;
import com.eyelevel.documentprocessor.service.s3.S3StorageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.net.URL;

/**
 * Service responsible for generating secure, pre-signed URLs for downloading processed files.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DownloadService {

    private final FileMasterRepository fileMasterRepository;
    private final GxMasterRepository gxMasterRepository;
    private final S3StorageService s3StorageService;

    /**
     * Generates a pre-signed download URL for a file identified by either a FileMaster or GxMaster ID.
     * The GxMaster ID is given priority if both are provided.
     * This method is OPTIMIZED to only fetch the S3 key, not the entire entity.
     *
     * @param request The request containing the ID of the file to download.
     * @return A DTO containing the generated pre-signed URL.
     */
    @Transactional(readOnly = true)
    public PresignedDownloadResponse generatePresignedDownloadUrl(DownloadFileRequest request) {
        final String s3Key;

        // Priority is given to gxMasterId, as it represents the final processed artifact.
        if (request.getGxMasterId() != null) {
            log.info("Generating download URL for GxMaster ID: {}", request.getGxMasterId());
            // OPTIMIZED: Fetch only the fileLocation string.
            s3Key = gxMasterRepository.findFileLocationById(request.getGxMasterId())
                    .orElseThrow(() -> new NotFoundException("GxMaster with ID " + request.getGxMasterId() + " not found."));
        } else {
            log.info("Generating download URL for FileMaster ID: {}", request.getFileMasterId());
            // OPTIMIZED: Fetch only the fileLocation string.
            s3Key = fileMasterRepository.findFileLocationById(request.getFileMasterId())
                    .orElseThrow(() -> new NotFoundException("FileMaster with ID " + request.getFileMasterId() + " not found."));
        }

        // Validate that we have a valid S3 key to use.
        if (!StringUtils.hasText(s3Key) || "N/A".equalsIgnoreCase(s3Key)) {
            throw new DocumentProcessingException("The requested file does not have a downloadable artifact. It may have been a duplicate or was ignored during processing.");
        }

        log.debug("Found S3 key '{}' for download.", s3Key);
        URL downloadUrl = s3StorageService.generatePresignedDownloadUrl(s3Key);

        return new PresignedDownloadResponse(downloadUrl);
    }
}