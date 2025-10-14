package com.eyelevel.documentprocessor.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.net.URL;

@Getter
@AllArgsConstructor
public class PresignedUploadResponse {
    private final Long jobId;
    private final URL uploadUrl;
}