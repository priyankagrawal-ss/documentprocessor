package com.eyelevel.documentprocessor.dto.gx.docupload.response;

import java.util.UUID;

/**
 * Represents the response from a document upload request to GX.
 *
 * @param ingest  The ingestion response details.
 * @param message A message describing the result of the operation.
 */
public record GXUploadDocumentResponse(UploadResponse ingest, String message) {

    /**
     * Contains details about the document ingestion process.
     *
     * @param processId The unique identifier for the ingestion process.
     * @param status    The current status of the ingestion process.
     */
    public record UploadResponse(UUID processId, String status) {
    }
}