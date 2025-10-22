package com.eyelevel.documentprocessor.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Defines the possible ingestion states for a document within the external GroundX system.
 * This enum includes a utility to safely convert string-based statuses from the API response.
 */
@Getter
@AllArgsConstructor
public enum GxStatus {
    QUEUED_FOR_UPLOAD("queued_for_upload"),
    QUEUED("queued"),
    PROCESSING("processing"),
    COMPLETE("complete"),
    ERROR("error"),
    CANCELLED("cancelled"),
    ACTIVE("active"),
    IN_ACTIVE("inactive"),
    SKIPPED("skipped");

    private static final Map<String, GxStatus> VALUE_MAP = Stream.of(values()).collect(
            Collectors.toMap(GxStatus::getValue, Function.identity()));
    private final String value;

    /**
     * Safely converts a string status from the GX API into its corresponding enum constant.
     * If the string does not match any known status, it defaults to {@code PROCESSING}
     * to ensure the system continues to track the item.
     *
     * @param value The string status received from the API.
     *
     * @return The matching {@link GxStatus} enum, or {@code PROCESSING} as a fallback.
     */
    public static GxStatus convertByValue(String value) {
        return VALUE_MAP.getOrDefault(value, PROCESSING);
    }
}