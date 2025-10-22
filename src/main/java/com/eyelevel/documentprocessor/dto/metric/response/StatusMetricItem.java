package com.eyelevel.documentprocessor.dto.metric.response;

/**
 * A Data Transfer Object representing a single status and its corresponding count.
 * <p>
 * Using a Java Record is a modern and concise way to create a simple, immutable data carrier.
 * It automatically provides a constructor, getters, equals(), hashCode(), and toString().
 *
 * @param status The name of the status (e.g., "Completed", "Total").
 * @param count  The number of documents with that status.
 */
public record StatusMetricItem(String status, Long count) {
}
