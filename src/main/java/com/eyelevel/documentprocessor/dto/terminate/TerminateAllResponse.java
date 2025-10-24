package com.eyelevel.documentprocessor.dto.terminate;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * A DTO representing the response from the bulk termination endpoint.
 * It provides a summary of the administrative action taken.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class TerminateAllResponse {

    /**
     * A human-readable message summarizing the result of the operation.
     * Example: "Termination signal sent to 15 active jobs and queues have been purged."
     */
    private String message;

    /**
     * The total number of jobs that were found in an active state and were
     * successfully marked for termination.
     */
    private int jobsTerminated;
}
