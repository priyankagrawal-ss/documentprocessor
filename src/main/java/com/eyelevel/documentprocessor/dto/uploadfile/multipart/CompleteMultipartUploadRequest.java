package com.eyelevel.documentprocessor.dto.uploadfile.multipart;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.services.s3.model.CompletedPart;

import java.util.List;

@Getter
@Setter
public class CompleteMultipartUploadRequest {

    @NotNull(message = "The 'jobId' field is required.")
    @Positive(message = "The 'jobId' must be a positive number.")
    private Long jobId;

    @NotBlank(message = "The 'uploadId' field cannot be empty.")
    private String uploadId;

    @NotEmpty(message = "The 'parts' list cannot be empty.")
    private List<CompletedPart> parts;
}