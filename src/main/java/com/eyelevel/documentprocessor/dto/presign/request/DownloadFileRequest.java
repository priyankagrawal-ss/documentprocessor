package com.eyelevel.documentprocessor.dto.presign.request;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Schema(description = "A DTO for requesting a presign url for download file. Exactly one of the IDs must be provided.")
public class DownloadFileRequest {
    @Schema(description = "The ID of a file processing task (FileMaster).", example = "105", nullable = true)
    private Long fileMasterId;

    @Schema(description = "The ID of a GX upload task (GxMaster).", example = "210", nullable = true)
    private Long gxMasterId;

    @JsonIgnore
    @AssertTrue(message = "Exactly one of 'fileMasterId' or 'gxMasterId' must be provided.")
    public boolean isExactlyOneIdPresent() {
        return (fileMasterId != null && gxMasterId == null) || (fileMasterId == null && gxMasterId != null);
    }
}
