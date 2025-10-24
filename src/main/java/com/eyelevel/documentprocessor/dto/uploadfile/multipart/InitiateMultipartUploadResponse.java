package com.eyelevel.documentprocessor.dto.uploadfile.multipart;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor // It's good practice to have a no-arg constructor
public class InitiateMultipartUploadResponse {

    private long jobId;
    private String uploadId;

}
