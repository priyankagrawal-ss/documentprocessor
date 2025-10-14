package com.eyelevel.documentprocessor.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Represents a ZIP archive that has been uploaded for processing.
 * It serves as the parent record for all {@link FileMaster} entities extracted from it.
 */
@Entity
@Table(name = "zip_master")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ZipMaster {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * The parent processing job associated with this ZIP file.
     */
    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "processing_job_id", nullable = false, unique = true)
    private ProcessingJob processingJob;

    /**
     * A list of all files that were extracted from this ZIP archive.
     */
    @OneToMany(mappedBy = "zipMaster", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<FileMaster> fileMasters;

    /**
     * For single ZIP uploads, this specifies the target GX bucket for all extracted files.
     * For bulk uploads, this will be null.
     */
    @Column
    private Integer gxBucketId;

    /**
     * The current status of the ZIP extraction process itself.
     */
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ZipProcessingStatus zipProcessingStatus;

    @Column(nullable = false)
    private String originalFilePath;

    @Column(nullable = false)
    private String originalFileName;

    private Long fileSize;

    @Column(columnDefinition = "TEXT")
    private String errorMessage;

    @CreationTimestamp
    @Column(updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;
}