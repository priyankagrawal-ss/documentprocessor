package com.eyelevel.documentprocessor.dto.retry.request;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.AssertTrue;
import lombok.Getter;
import lombok.Setter;

/**
 * A DTO for requesting a retry of a failed processing task.
 * The client must provide exactly one of fileMasterId or gxMasterId.
 */
@Getter
@Setter
public class RetryRequest {

    private Long fileMasterId;
    private Long gxMasterId;

    @JsonIgnore
    @AssertTrue(message = "Exactly one of 'fileMasterId' or 'gxMasterId' must be provided.")
    public boolean isExactlyOneIdPresent() {
        // This validation ensures that the request is not ambiguous.
        return (fileMasterId != null && gxMasterId == null) || (fileMasterId == null && gxMasterId != null);
    }
}
