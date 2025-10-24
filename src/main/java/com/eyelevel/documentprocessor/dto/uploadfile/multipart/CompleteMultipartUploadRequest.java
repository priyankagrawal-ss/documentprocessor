package com.eyelevel.documentprocessor.dto.uploadfile.multipart;

import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.services.s3.model.CompletedPart;

import java.util.List;

@Getter
@Setter
public class CompleteMultipartUploadRequest {
    private Long jobId;
    private String uploadId;
    private List<CompletedPart> parts;
}
