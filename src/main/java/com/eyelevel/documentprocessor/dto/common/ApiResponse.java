package com.eyelevel.documentprocessor.dto.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Getter;

/**
 * A standardized, generic wrapper for all API responses.
 * It provides a consistent structure for both successful and failed responses,
 * making it easy for clients (like the UI) to handle them.
 **/
@Getter
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiResponse<T> {
    /**
     * A message to display to the user.
     */
    private final String displayMessage;

    /**
     * The response data.
     */
    private final T response;

    /**
     * A flag indicating whether to show the display message.
     */
    private final Boolean showMessage;

    /**
     * The HTTP status code of the response.
     */
    private final Integer statusCode;
}
