package com.eyelevel.documentprocessor.dto.gx.docupload;

/**
 * Represents the parameters for a document upload to GX.
 *
 * @param bucketId  The ID of the bucket to upload the document to.
 * @param fileName  The name of the file being uploaded.
 * @param fileType  The MIME type of the file.
 * @param sourceUrl The source URL of the file.
 */
public record GXDocumentUploadParameters(Integer bucketId, String fileName, String fileType, String sourceUrl) {
}