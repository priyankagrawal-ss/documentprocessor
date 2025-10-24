package com.eyelevel.documentprocessor.dto.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;

/**
 * A standardized, generic wrapper for all API responses.
 * It provides a consistent structure for both successful and failed responses,
 * making it easy for clients (like the UI) to handle them.
 *
 * @param <T> The type of the data payload for successful responses.
 */
@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiResponse<T> {

    private boolean success;
    private long timestamp;
    private String message;
    private T data;
    private String errorDetails;

    // Private constructor to force usage of static factory methods
    private ApiResponse() {
        this.timestamp = Instant.now().toEpochMilli();
    }

    // --- Static Factory Methods for SUCCESS cases ---

    /**
     * Creates a successful API response with a data payload and a message.
     */
    public static <T> ApiResponse<T> success(T data, String message) {
        ApiResponse<T> response = new ApiResponse<>();
        response.setSuccess(true);
        response.setData(data);
        response.setMessage(message);
        return response;
    }

    /**
     * Creates a successful API response with just a data payload.
     */
    public static <T> ApiResponse<T> success(T data) {
        return success(data, "Request was successful.");
    }

    // --- Static Factory Methods for ERROR cases ---

    /**
     * Creates a failed API response with a message and detailed error info.
     */
    public static <T> ApiResponse<T> error(String message, String errorDetails) {
        ApiResponse<T> response = new ApiResponse<>();
        response.setSuccess(false);
        response.setMessage(message);
        response.setErrorDetails(errorDetails);
        return response;
    }

    /**
     * Creates a simple failed API response with just a message.
     */
    public static <T> ApiResponse<T> error(String message) {
        return error(message, null);
    }
}
