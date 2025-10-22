package com.eyelevel.documentprocessor.dto.metric.request;

import lombok.Data;

import java.util.List;

@Data
public class MetricsRequest {
    private List<Integer> gxBucketIds;
}
