package com.eyelevel.documentprocessor.dto.metric;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A DTO to map the results from the native status metrics query.
 * Must have a constructor that matches the columns defined in the @SqlResultSetMapping.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class StatusMetric {
    private Integer gxBucketId;
    private String displayStatus;
    private Long statusCount;
}