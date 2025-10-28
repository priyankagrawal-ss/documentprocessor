package com.eyelevel.documentprocessor.dto.metric;

import com.eyelevel.documentprocessor.model.ZipProcessingStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


/**
 * DTO to hold the status and filename from a ZipMaster query.
 * The constructor signature MUST match the one used in the JPQL query.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ZipStatusFileNameDto {
    private ZipProcessingStatus status;
    private String filename;
}
