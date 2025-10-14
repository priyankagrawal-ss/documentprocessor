package com.eyelevel.documentprocessor.model;

import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Entity
@Data
@Table(name = "processing_job")
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

    @Column
    private Integer gxBucketId;

    @Column(nullable = false)
    private boolean skipGxProcess = false;

    /**
     * A transient helper method to determine if this job is a bulk upload.
     * A job is considered a bulk upload if no gxBucketId was provided upon creation.
     *
     * @return true if it is a bulk upload, false otherwise.
     */
    @Transient
    public boolean isBulkUpload() {
        return this.gxBucketId == null;
    }
}