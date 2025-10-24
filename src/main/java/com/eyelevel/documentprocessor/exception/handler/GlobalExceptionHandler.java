package com.eyelevel.documentprocessor.exception.handler;

import com.eyelevel.documentprocessor.dto.common.ApiResponse;
import com.eyelevel.documentprocessor.exception.DocumentProcessingException;
import com.eyelevel.documentprocessor.exception.FileProtectedException;
import com.eyelevel.documentprocessor.exception.RetryFailedException;
import com.eyelevel.documentprocessor.exception.apiclient.*;
import com.eyelevel.documentprocessor.exception.json.JsonParsingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.stream.Collectors;

/**
 * A centralized exception handler for the entire application.
 * It intercepts exceptions thrown from controllers and converts them into a
 * standardized ApiResponse format with the semantically correct HTTP status code.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    // --- 4xx Client Error Handlers ---

    /**
     * Handles specific client-side errors related to business logic. (400 Bad Request)
     */
    @ExceptionHandler({DocumentProcessingException.class,
                       FileProtectedException.class,
                       JsonParsingException.class,
                       RetryFailedException.class,
                       BadRequestException.class})
    public ResponseEntity<ApiResponse<Object>> handleBadRequest(RuntimeException ex) {
        log.warn("Bad Request Exception: {}", ex.getMessage());
        ApiResponse<Object> response = ApiResponse.error(ex.getMessage());
        return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
    }

    /**
     * Handles unauthorized access errors. (401 Unauthorized)
     */
    @ExceptionHandler(UnauthorizedException.class)
    public ResponseEntity<ApiResponse<Object>> handleUnauthorized(UnauthorizedException ex) {
        log.warn("Unauthorized Access Exception: {}", ex.getMessage());
        ApiResponse<Object> response = ApiResponse.error(ex.getMessage());
        return new ResponseEntity<>(response, HttpStatus.UNAUTHORIZED);
    }

    /**
     * Handles forbidden access errors. (403 Forbidden)
     */
    @ExceptionHandler(ForbiddenException.class)
    public ResponseEntity<ApiResponse<Object>> handleForbidden(ForbiddenException ex) {
        log.warn("Forbidden Access Exception: {}", ex.getMessage());
        ApiResponse<Object> response = ApiResponse.error(ex.getMessage());
        return new ResponseEntity<>(response, HttpStatus.FORBIDDEN);
    }

    /**
     * Handles resource not found errors. (404 Not Found)
     */
    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<ApiResponse<Object>> handleNotFound(NotFoundException ex) {
        log.warn("Resource Not Found Exception: {}", ex.getMessage());
        ApiResponse<Object> response = ApiResponse.error(ex.getMessage());
        return new ResponseEntity<>(response, HttpStatus.NOT_FOUND);
    }

    /**
     * Handles resource conflict errors (e.g., creating a resource that already exists). (409 Conflict)
     */
    @ExceptionHandler(ConflictException.class)
    public ResponseEntity<ApiResponse<Object>> handleConflict(ConflictException ex) {
        log.warn("Conflict Exception: {}", ex.getMessage());
        ApiResponse<Object> response = ApiResponse.error(ex.getMessage());
        return new ResponseEntity<>(response, HttpStatus.CONFLICT);
    }

    /**
     * Handles rate-limiting errors. (429 Too Many Requests)
     */
    @ExceptionHandler(TooManyRequestsException.class)
    public ResponseEntity<ApiResponse<Object>> handleTooManyRequests(TooManyRequestsException ex) {
        log.warn("Too Many Requests Exception: {}", ex.getMessage());
        ApiResponse<Object> response = ApiResponse.error(ex.getMessage());
        return new ResponseEntity<>(response, HttpStatus.TOO_MANY_REQUESTS);
    }

    /**
     * Handles validation errors from @Valid on request bodies. (400 Bad Request)
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiResponse<Object>> handleValidationExceptions(MethodArgumentNotValidException ex) {
        String errors = ex.getBindingResult().getFieldErrors().stream()
                          .map(error -> String.format("'%s': %s", error.getField(), error.getDefaultMessage()))
                          .collect(Collectors.joining(", "));
        String errorMessage = "Validation failed: " + errors;
        log.warn("Handling validation exception: {}", errorMessage);
        ApiResponse<Object> response = ApiResponse.error("Invalid input provided.", errorMessage);
        return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
    }

    /**
     * Handles type mismatch errors for path variables or request parameters (e.g., string for a Long). (400 Bad Request)
     */
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ApiResponse<Object>> handleTypeMismatch(MethodArgumentTypeMismatchException ex) {
        String errorMessage = String.format("Invalid value '%s' for parameter '%s'. Expected type '%s'.", ex.getValue(),
                                            ex.getName(), ex.getRequiredType() != null
                                                          ? ex.getRequiredType().getSimpleName()
                                                          : String.valueOf(ex.getRequiredType()));
        log.warn("Handling type mismatch exception: {}", errorMessage);
        ApiResponse<Object> response = ApiResponse.error("Invalid parameter type provided.", errorMessage);
        return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
    }


    // --- 5xx Server Error Handlers ---

    /**
     * Handles bad gateway errors from downstream services. (502 Bad Gateway)
     */
    @ExceptionHandler(BadGatewayException.class)
    public ResponseEntity<ApiResponse<Object>> handleBadGateway(BadGatewayException ex) {
        log.error("Bad Gateway Exception: {}", ex.getMessage());
        ApiResponse<Object> response = ApiResponse.error(ex.getMessage());
        return new ResponseEntity<>(response, HttpStatus.BAD_GATEWAY);
    }

    /**
     * Handles service unavailable errors from downstream services. (503 Service Unavailable)
     */
    @ExceptionHandler(ServiceUnavailableException.class)
    public ResponseEntity<ApiResponse<Object>> handleServiceUnavailable(ServiceUnavailableException ex) {
        log.error("Service Unavailable Exception: {}", ex.getMessage());
        ApiResponse<Object> response = ApiResponse.error(ex.getMessage());
        return new ResponseEntity<>(response, HttpStatus.SERVICE_UNAVAILABLE);
    }

    /**
     * Handles gateway timeout errors from downstream services. (504 Gateway Timeout)
     */
    @ExceptionHandler(GatewayTimeoutException.class)
    public ResponseEntity<ApiResponse<Object>> handleGatewayTimeout(GatewayTimeoutException ex) {
        log.error("Gateway Timeout Exception: {}", ex.getMessage());
        ApiResponse<Object> response = ApiResponse.error(ex.getMessage());
        return new ResponseEntity<>(response, HttpStatus.GATEWAY_TIMEOUT);
    }

    /**
     * A final catch-all handler for any other unexpected exceptions. (500 Internal Server Error)
     * This includes InternalServerException and any other RuntimeException.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Object>> handleGenericException(Exception ex) {
        log.error("An unexpected internal server error occurred", ex);
        ApiResponse<Object> response = ApiResponse.error(
                "An unexpected internal error occurred. Please contact support.", ex.getClass().getSimpleName());
        return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
