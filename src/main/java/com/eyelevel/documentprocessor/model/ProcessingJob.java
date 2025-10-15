package com.eyelevel.documentprocessor.model;

import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

/**
 * Represents the top-level entity for a document processing request.
 * A job can represent a single file upload or a bulk (ZIP) upload.
 */
@Entity
@Table(name = "processing_job")
@Data
public class ProcessingJob {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String originalFilename;

    @Column(nullable = false)
    private String fileLocation;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ProcessingStatus status;

    @Column
    private String currentStage;

    @Column(columnDefinition = "TEXT")
    private String errorMessage;

    @CreationTimestamp
    @Column(updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;

    /**
     * The GroundX bucket identifier for this job. If null, the job is considered a bulk upload,
     * where bucket information is derived from the ZIP file's internal structure.
     */
    @Column
    private Integer gxBucketId;

    /**
     * A flag indicating whether the final upload to the GroundX service should be skipped.
     */
    @Column(nullable = false)
    private boolean skipGxProcess = false;
    
    /**
     * A field to store a summary message, typically used when a job finishes
     * with a PARTIAL_SUCCESS status to explain what succeeded and what failed.
     */
    @Column(columnDefinition = "TEXT")
    private String remark;

    /**
     * A transient helper method to determine if this job is a bulk upload.
     *
     * @return true if {@code gxBucketId} is null, false otherwise.
     */
    @Transient
    public boolean isBulkUpload() {
        return this.gxBucketId == null;
    }
}