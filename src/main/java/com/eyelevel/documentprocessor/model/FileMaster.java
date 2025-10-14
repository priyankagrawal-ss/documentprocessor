package com.eyelevel.documentprocessor.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

/**
 * Represents a single file to be processed by the document pipeline.
 * Each record tracks the file's location, status, metadata, and relationship to its parent job.
 */
@Entity
@Table(name = "file_master")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileMaster {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * The parent ZIP archive this file was extracted from. Null if the file was a single upload.
     */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "zip_master_id")
    private ZipMaster zipMaster;

    /**
     * The parent processing job that this file belongs to. This is a non-null, mandatory relationship.
     */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "processing_job_id", nullable = false)
    private ProcessingJob processingJob;

    /**
     * The GroundX bucket identifier associated with this file.
     */
    @Column(nullable = false)
    private Integer gxBucketId;

    /**
     * If this file is a duplicate, this field stores the ID of the original, completed FileMaster record.
     */
    private Long duplicateOfFileId;

    /**
     * The full S3 key where the file's content is stored.
     */
    @Column(nullable = false)
    private String fileLocation;

    /**
     * The original name of the file.
     */
    @Column(nullable = false)
    private String fileName;

    /**
     * The size of the file in bytes.
     */
    private Long fileSize;

    /**
     * The file's extension (e.g., "pdf", "docx").
     */
    private String extension;

    /**
     * The SHA-256 hash of the file's content, used for duplicate detection.
     */
    private String fileHash;

    /**
     * The current processing status of this specific file.
     */
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private FileProcessingStatus fileProcessingStatus;

    /**
     * Stores any error message if the file processing failed.
     */
    @Column(columnDefinition = "TEXT")
    private String errorMessage;

    @CreationTimestamp
    @Column(updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;
}