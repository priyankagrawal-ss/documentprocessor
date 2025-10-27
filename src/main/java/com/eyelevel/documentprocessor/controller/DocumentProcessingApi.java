package com.eyelevel.documentprocessor.controller;

import com.eyelevel.documentprocessor.dto.common.ApiResponse;
import com.eyelevel.documentprocessor.dto.metric.request.MetricsRequest;
import com.eyelevel.documentprocessor.dto.metric.response.StatusMetricItem;
import com.eyelevel.documentprocessor.dto.presign.download.PresignedDownloadResponse;
import com.eyelevel.documentprocessor.dto.presign.request.DownloadFileRequest;
import com.eyelevel.documentprocessor.dto.retry.request.RetryRequest;
import com.eyelevel.documentprocessor.dto.terminate.TerminateAllResponse;
import com.eyelevel.documentprocessor.dto.uploadfile.direct.PresignedUploadResponse;
import com.eyelevel.documentprocessor.dto.uploadfile.multipart.CompleteMultipartUploadRequest;
import com.eyelevel.documentprocessor.dto.uploadfile.multipart.InitiateMultipartUploadResponse;
import com.eyelevel.documentprocessor.dto.uploadfile.multipart.PresignedUrlPartResponse;
import com.eyelevel.documentprocessor.view.DocumentProcessingView;
import com.smartsensesolutions.commons.dao.filter.FilterRequest;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Map;

@Tag(name = "Document Processing Workflow", description = "Endpoints for managing the entire document processing lifecycle, from upload to status monitoring.")
public interface DocumentProcessingApi {

    @Operation(summary = "Create Direct Upload URL",
            description = "Initiates a new processing job and generates a pre-signed S3 URL for a direct, single-part file upload. This is suitable for smaller files.")
    @ApiResponses(value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "Pre-signed URL generated successfully.",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = PresignedUploadResponse.class),
                            examples = @ExampleObject(value = """
                                    {
                                        "success": true,
                                        "timestamp": 1672531200000,
                                        "message": "Pre-signed URL for direct upload generated successfully.",
                                        "data": {
                                            "jobId": 1,
                                            "uploadUrl": "https://your-bucket.s3.region.amazonaws.com/..."
                                        }
                                    }
                                    """))),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "400", description = "Bad Request - Missing or invalid fileName.",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ApiResponse.class))),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "500", description = "Internal Server Error",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ApiResponse.class)))
    })
    ResponseEntity<ApiResponse<PresignedUploadResponse>> createDirectUploadUrl(
            @Parameter(description = "The original name of the file to be uploaded.", required = true, example = "document.pdf")
            @RequestParam("fileName") String fileName,
            @Parameter(description = "Optional identifier for grouping documents in the target system.", example = "12345")
            @RequestParam(value = "gxBucketId", required = false) Integer gxBucketId,
            @Parameter(description = "If true, bypasses the final upload step to the GroundX system.")
            @RequestParam(value = "skipGxProcess", defaultValue = "false") boolean skipGxProcess);

    @Operation(summary = "Initiate Multipart Upload",
            description = "Initiates a multipart upload for a large file. This is the first step in the large file upload workflow, creating a job and returning a unique Upload ID.")
    @ApiResponses(value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "Multipart upload initiated successfully.",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = InitiateMultipartUploadResponse.class),
                            examples = @ExampleObject(value = """
                                    {
                                        "success": true,
                                        "timestamp": 1672531200000,
                                        "message": "Multipart upload initiated successfully.",
                                        "data": {
                                            "jobId": 2,
                                            "uploadId": "TehG5i3g.T2123s5543d.d12..."
                                        }
                                    }
                                    """))),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "400", description = "Bad Request - Missing or invalid fileName.",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ApiResponse.class))),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "500", description = "Internal Server Error",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ApiResponse.class)))
    })
    ResponseEntity<ApiResponse<InitiateMultipartUploadResponse>> initiateMultipartUpload(
            @Parameter(description = "The original name of the file to be uploaded.", required = true, example = "large-archive.zip")
            @RequestParam("fileName") String fileName,
            @Parameter(description = "Optional identifier for grouping documents in the target system.", example = "12345")
            @RequestParam(value = "gxBucketId", required = false) Integer gxBucketId,
            @Parameter(description = "If true, bypasses the final upload step to the GroundX system.")
            @RequestParam(value = "skipGxProcess", defaultValue = "false") boolean skipGxProcess);

    @Operation(summary = "Get Pre-signed URL for a File Part",
            description = "Generates a pre-signed URL for a single part (chunk) of a multipart upload. The client uses this URL to upload one chunk of the file.")
    @ApiResponses(value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "Pre-signed URL for the part generated successfully.",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = PresignedUrlPartResponse.class),
                            examples = @ExampleObject(value = """
                                    {
                                        "success": true,
                                        "timestamp": 1672531200000,
                                        "message": "Request was successful.",
                                        "data": {
                                            "presignedUrl": "https://your-bucket.s3.region.amazonaws.com/...?partNumber=1&uploadId=..."
                                        }
                                    }
                                    """))),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "400", description = "Bad Request - Invalid or missing parameters.",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ApiResponse.class))),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "404", description = "Not Found - The specified job ID does not exist.",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ApiResponse.class)))
    })
    ResponseEntity<ApiResponse<PresignedUrlPartResponse>> getPresignedUrlForPart(
            @Parameter(description = "The ID of the job initiated in the first step.", required = true, example = "1")
            @PathVariable Long jobId,
            @Parameter(description = "The sequential number of the file chunk (must be between 1 and 10000).", required = true, example = "1")
            @PathVariable int partNumber,
            @Parameter(description = "The S3 Upload ID returned from the initiation step.", required = true)
            @RequestParam("uploadId") String uploadId);

    @Operation(summary = "Complete a Multipart Upload",
            description = "Finalizes a multipart upload after all parts have been successfully uploaded to S3. This signals to S3 to reassemble the parts into a single file.")
    @ApiResponses(value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "Multipart upload completed successfully.",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = ApiResponse.class),
                            examples = @ExampleObject(value = """
                                    {
                                        "success": true,
                                        "timestamp": 1672531200000,
                                        "message": "Multipart upload completed successfully.",
                                        "data": null
                                    }
                                    """))),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "400", description = "Bad Request - Invalid request body or parameters.",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ApiResponse.class))),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "404", description = "Not Found - The specified job ID does not exist.",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ApiResponse.class)))
    })
    ResponseEntity<ApiResponse<Void>> completeMultipartUpload(
            @Parameter(description = "The ID of the job being completed.", required = true, example = "1")
            @PathVariable Long jobId,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "Request body containing the uploadId and the list of completed parts with their ETags.",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = CompleteMultipartUploadRequest.class),
                            examples = @ExampleObject(value = """
                                    {
                                        "jobId": 2,
                                        "uploadId": "TehG5i3g.T2123s5543d.d12...",
                                        "parts": [
                                            {
                                                "partNumber": 1,
                                                "eTag": "\\"d41d8cd98f00b204e9800998ecf8427e\\""
                                            },
                                            {
                                                "partNumber": 2,
                                                "eTag": "\\"f427e1d8cd98f00b204e9800998ecf8d\\""
                                            }
                                        ]
                                    }
                                    """)))
            @RequestBody CompleteMultipartUploadRequest request);

    @Operation(summary = "Trigger Backend Processing",
            description = "Signals the system to start processing a file that has been successfully uploaded. This enqueues the job for asynchronous processing.")
    @ApiResponses(value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "202", description = "Processing has been accepted and queued.",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = ApiResponse.class),
                            examples = @ExampleObject(value = """
                                    {
                                        "success": true,
                                        "timestamp": 1672531200000,
                                        "message": "Processing has been accepted and queued.",
                                        "data": null
                                    }
                                    """))),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "400", description = "Bad Request - The job is not in a triggerable state.",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ApiResponse.class))),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "404", description = "Not Found - The specified job ID does not exist.",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ApiResponse.class)))
    })
    ResponseEntity<ApiResponse<Void>> startProcessing(
            @Parameter(description = "The unique identifier of the job to trigger.", required = true, example = "1")
            @PathVariable Long jobId);

    @Operation(summary = "Retry a Failed Task",
            description = "Requests a retry for a specific part of a job that has failed. The client must provide either a 'fileMasterId' (for a file processing failure) or a 'gxMasterId' (for an upload-to-GX failure).")
    @ApiResponses(value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "202", description = "Retry request accepted and the task has been re-queued.",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = ApiResponse.class),
                            examples = @ExampleObject(value = """
                                    {
                                        "success": true,
                                        "timestamp": 1672531200000,
                                        "message": "Retry request accepted and the task has been re-queued.",
                                        "data": null
                                    }
                                    """))),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "400", description = "Bad Request - Invalid request body or the task is not in a retryable state.",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ApiResponse.class))),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "404", description = "Not Found - The specified task ID does not exist.",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ApiResponse.class)))
    })
    ResponseEntity<ApiResponse<Void>> retryFailedTask(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The request body containing the ID of the failed task. Exactly one of 'fileMasterId' or 'gxMasterId' must be provided.", required = true,
                    content = @Content(schema = @Schema(implementation = RetryRequest.class),
                            examples = @ExampleObject(name = "Retry File Processing", value = """
                                    {
                                        "fileMasterId": 105
                                    }
                                    """)))
            @Valid @RequestBody RetryRequest retryRequest);

    @Operation(summary = "[ADMIN] Terminate a Single Job",
            description = "Terminates a single active job and attempts to stop all related processing. This is an administrative action and should be used with caution.")
    @ApiResponses(value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "202", description = "Termination request has been accepted.",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = ApiResponse.class),
                            examples = @ExampleObject(value = """
                                    {
                                        "success": true,
                                        "timestamp": 1672531200000,
                                        "message": "Termination request for job ID 1 has been accepted.",
                                        "data": null
                                    }
                                    """))),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "404", description = "Not Found - The specified job ID does not exist.",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ApiResponse.class)))
    })
    ResponseEntity<ApiResponse<Void>> terminateJob(
            @Parameter(description = "The unique identifier of the job to terminate.", required = true, example = "1")
            @PathVariable Long jobId);

    @Operation(summary = "[ADMIN] Terminate All Active Jobs",
            description = "Terminates ALL active jobs currently in the system and purges the SQS queues. This is a powerful administrative action for system-wide stops.")
    @ApiResponses(value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "Termination signal sent successfully.",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = TerminateAllResponse.class),
                            examples = @ExampleObject(value = """
                                    {
                                        "success": true,
                                        "timestamp": 1672531200000,
                                        "message": "Request was successful.",
                                        "data": {
                                            "message": "Termination signal sent to 15 active jobs and queues have been purged.",
                                            "jobsTerminated": 15
                                        }
                                    }
                                    """)))
    })
    ResponseEntity<ApiResponse<TerminateAllResponse>> terminateAllActiveJobs();

    @Operation(summary = "List and Filter Documents",
            description = "Retrieves a paginated and filtered list of documents for a specific bucket, based on their processing status and other criteria.")
    @ApiResponses(value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "Document list retrieved successfully.",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = Page.class),
                            examples = @ExampleObject(value = """
                                    {
                                        "success": true,
                                        "timestamp": 1672531200000,
                                        "message": "Request was successful.",
                                        "data": {
                                            "content": [
                                                {
                                                    "id": 101,
                                                    "fileMasterId": 101,
                                                    "gxMasterId": 201,
                                                    "zipFileName": "annual_reports.zip",
                                                    "fileName": "Q1_report.pdf",
                                                    "extension": "pdf",
                                                    "fileSize": 123456,
                                                    "gxBucketId": 12345,
                                                    "status": "Completed",
                                                    "processingStage": "Upload to GX Complete",
                                                    "error": null,
                                                    "createdAt": "2025-10-27T10:00:00Z"
                                                }
                                            ],
                                            "pageable": {
                                                "pageNumber": 0,
                                                "pageSize": 10
                                            },
                                            "totalElements": 1,
                                            "totalPages": 1,
                                            "last": true
                                        }
                                    }
                                    """))),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "400", description = "Bad Request - Invalid filter request.",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ApiResponse.class)))
    })
    ResponseEntity<ApiResponse<Page<DocumentProcessingView>>> listDocuments(
            @Parameter(description = "The identifier of the bucket to list documents from.", required = true, example = "12345")
            @PathVariable("gxBucketId") Integer gxBucketId,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    description = """
                            Request body for filtering, pagination, and sorting.
                            - `criteria`: A list of filters combined with AND.
                            - `orCriteria`: A list of filters combined with OR.
                            - `criteriaOperator`: (AND | OR) - How to combine the `criteria` and `orCriteria` blocks.
                            - **Available Operators**: `EQUALS`, `NOT_EQUAL`, `CONTAIN`, `NOT_CONTAIN`, `IN`, `NOT_IN`, `GREATER_THAN`, `LESSER_THAN`, `GREATER_EQUALS`, `LESSER_EQUALS`, `NULL`, `NOT_NULL`
                            """,
                    required = true,
                    content = @Content(schema = @Schema(implementation = FilterRequest.class),
                            examples = {
                                    @ExampleObject(name = "Simple Pagination and Sort",
                                            summary = "Get page 0, 10 items, sorted by creation date",
                                            value = """
                                                    {
                                                      "page": 0,
                                                      "size": 10,
                                                      "sort": [
                                                        {
                                                          "column": "createdAt",
                                                          "sortType": "DESC"
                                                        }
                                                      ]
                                                    }
                                                    """),
                                    @ExampleObject(name = "Filter by Status",
                                            summary = "Find all 'Completed' documents",
                                            value = """
                                                    {
                                                      "page": 0,
                                                      "size": 20,
                                                      "criteria": [
                                                        {
                                                          "column": "status",
                                                          "operator": "EQUALS",
                                                          "values": ["Completed"]
                                                        }
                                                      ]
                                                    }
                                                    """),
                                    @ExampleObject(name = "Multiple 'AND' Filters",
                                            summary = "Find PDF files with 'report' in the name",
                                            value = """
                                                    {
                                                      "page": 0,
                                                      "size": 10,
                                                      "criteria": [
                                                        {
                                                          "column": "fileName",
                                                          "operator": "CONTAIN",
                                                          "values": ["report"]
                                                        },
                                                        {
                                                          "column": "extension",
                                                          "operator": "EQUALS",
                                                          "values": ["pdf"]
                                                        }
                                                      ]
                                                    }
                                                    """),
                                    @ExampleObject(name = "Using 'OR' Logic",
                                            summary = "Find documents that are 'Failed' OR have 'timeout' in the error message",
                                            value = """
                                                    {
                                                      "page": 0,
                                                      "size": 50,
                                                      "orCriteria": [
                                                        {
                                                          "column": "status",
                                                          "operator": "EQUALS",
                                                          "values": ["Failed"]
                                                        },
                                                        {
                                                          "column": "error",
                                                          "operator": "CONTAIN",
                                                          "values": ["timeout"]
                                                        }
                                                      ]
                                                    }
                                                    """),
                                    @ExampleObject(name = "Complex 'AND' + 'OR' Query",
                                            summary = "Find PDF or DOCX files WHERE the name contains 'Q1' or 'Q2'",
                                            value = """
                                                    {
                                                      "page": 0,
                                                      "size": 10,
                                                      "criteriaOperator": "AND",
                                                      "criteria": [
                                                        {
                                                          "column": "extension",
                                                          "operator": "IN",
                                                          "values": ["pdf", "docx"]
                                                        }
                                                      ],
                                                      "orCriteria": [
                                                        {
                                                          "column": "fileName",
                                                          "operator": "CONTAIN",
                                                          "values": ["Q1"]
                                                        },
                                                        {
                                                          "column": "fileName",
                                                          "operator": "CONTAIN",
                                                          "values": ["Q2"]
                                                        }
                                                      ]
                                                    }
                                                    """)
                            }))
            @RequestBody FilterRequest filterRequest);

    @Operation(summary = "Get Document Status Metrics",
            description = "Retrieves a summary of document counts grouped by their processing status for a given list of bucket IDs.")
    @ApiResponses(value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "Metrics retrieved successfully.",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = Map.class),
                            examples = @ExampleObject(value = """
                                    {
                                        "success": true,
                                        "timestamp": 1672531200000,
                                        "message": "Request was successful.",
                                        "data": {
                                            "12345": [
                                                { "status": "Completed", "count": 50 },
                                                { "status": "Failed", "count": 2 },
                                                { "status": "Processing", "count": 5 }
                                            ],
                                            "67890": [
                                                { "status": "Completed", "count": 120 },
                                                { "status": "Failed", "count": 10 }
                                            ]
                                        }
                                    }
                                    """))),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "400", description = "Bad Request - Invalid request body.",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ApiResponse.class)))
    })
    ResponseEntity<ApiResponse<Map<Integer, List<StatusMetricItem>>>> getDocumentMetrics(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "A JSON object containing a list of gxBucketIds.", required = true,
                    content = @Content(schema = @Schema(implementation = MetricsRequest.class),
                            examples = @ExampleObject(value = """
                                    {
                                        "gxBucketIds": [12345, 67890]
                                    }
                                    """)))
            @RequestBody MetricsRequest request);

    @Operation(summary = "Generate Pre-signed Download URL",
            description = "Generates a temporary, secure, pre-signed URL for downloading a processed file. The request must specify either a 'fileMasterId' or a 'gxMasterId'. If 'gxMasterId' is provided, it will be used as the priority source.")
    @ApiResponses(value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "Pre-signed download URL generated successfully.",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = PresignedDownloadResponse.class),
                            examples = @ExampleObject(value = """
                                    {
                                        "success": true,
                                        "timestamp": 1672531200000,
                                        "message": "Request was successful.",
                                        "data": {
                                            "downloadUrl": "https://your-bucket.s3.region.amazonaws.com/..."
                                        }
                                    }
                                    """))),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "400", description = "Bad Request - Invalid request body or the specified file is not available for download.",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ApiResponse.class))),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "404", description = "Not Found - The specified file or job ID does not exist.",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ApiResponse.class)))
    })
    ResponseEntity<ApiResponse<PresignedDownloadResponse>> generatePresignedDownloadUrl(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The request body containing the ID of the file to download. Exactly one of 'fileMasterId' or 'gxMasterId' must be provided.", required = true,
                    content = @Content(schema = @Schema(implementation = RetryRequest.class),
                            examples = {
                                    @ExampleObject(name = "Download from GxMaster (Preferred)", value = """
                                            {
                                                "gxMasterId": 210
                                            }
                                            """),
                                    @ExampleObject(name = "Download from FileMaster", value = """
                                            {
                                                "fileMasterId": 105
                                            }
                                            """)
                            }))
            @Valid @RequestBody DownloadFileRequest request);
}