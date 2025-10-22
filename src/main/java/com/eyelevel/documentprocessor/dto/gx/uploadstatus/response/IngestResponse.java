package com.eyelevel.documentprocessor.dto.gx.uploadstatus.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Represents the detailed response of a document ingestion process from GX.
 *
 * @param ingest The details of the ingestion process.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record IngestResponse(IngestDetails ingest) {

    /**
     * Contains the core details of the ingestion process.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record IngestDetails(Long id, UUID processId, Progress progress, String status, String statusMessage) {
    }

    /**
     * Represents the progress of the document ingestion.
     */
    public record Progress(ProgressCategory cancelled,
                           ProgressCategory complete,
                           ProgressCategory errors,
                           ProgressCategory processing) {
    }

    /**
     * Represents a category of documents within the ingestion process.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ProgressCategory(List<Document> documents, int total) {
    }

    /**
     * Represents a single document's details within the ingestion process.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Document(long bucketId,
                           String documentId,
                           String fileName,
                           String fileSize,
                           String fileType,
                           Map<String, Object> filter,
                           UUID processId,
                           Map<String, Object> searchData,
                           String sourceUrl,
                           String status,
                           String statusMessage,
                           String xrayUrl) {
    }
}