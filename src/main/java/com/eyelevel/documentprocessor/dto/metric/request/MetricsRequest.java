package com.eyelevel.documentprocessor.dto.metric.request;

import jakarta.validation.constraints.NotEmpty;
import lombok.Data;

import java.util.List;

@Data
public class MetricsRequest {

    @NotEmpty(message = "The 'gxBucketIds' list cannot be null or empty.")
    private List<Integer> gxBucketIds;
}