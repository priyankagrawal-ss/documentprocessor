package com.eyelevel.documentprocessor.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
@AllArgsConstructor
public enum GxStatus {
    DUPLICATE("duplicate"),
    READING("reading"),
    QUEUED_FOR_UPLOAD("queued_for_upload"),
    QUEUED("queued"),
    PROCESSING("processing"),
    COMPLETE("complete"),
    ERROR("error"),
    CANCELLED("cancelled"),
    ACTIVE("active"),
    IN_ACTIVE("inactive"),
    SKIPPED("skipped"),
    IGNORED("ignored"),
    TERMINATED("terminated");


    private static final Map<String, GxStatus> VALUE_MAP =
            Stream.of(values())
                    .collect(Collectors.toMap(GxStatus::getValue, Function.identity()));
    private String value;

    public static GxStatus convertByValue(String value) {
        GxStatus gxStatus = VALUE_MAP.get(value);
        if (gxStatus == null) {
            return PROCESSING;
        }
        return gxStatus;
    }
}