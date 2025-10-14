package com.eyelevel.documentprocessor.dto.gx.docupload.request;

import java.util.List;

/**
 * Represents the request body for uploading one or more documents to GX.
 *
 * @param documents A list of documents to be uploaded.
 */
public record GXUploadFileRequest(List<DocumentRequest> documents) {

    /**
     * Represents a single document to be uploaded.
     *
     * @param bucketId  The ID of the bucket for the document.
     * @param fileName  The name of the file.
     * @param fileType  The MIME type of the file.
     * @param sourceUrl The source URL of the file.
     */
    public record DocumentRequest(
            Integer bucketId,
            String fileName,
            String fileType,
            String sourceUrl
    ) {
    }
}