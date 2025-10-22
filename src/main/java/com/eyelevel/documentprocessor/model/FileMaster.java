package com.eyelevel.documentprocessor.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

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

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "zip_master_id")
    private ZipMaster zipMaster;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "processing_job_id", nullable = false)
    private ProcessingJob processingJob;

    @Column(nullable = false)
    private Integer gxBucketId;

    private Long duplicateOfFileId;

    @Column(nullable = false)
    private String fileLocation;

    @Column(nullable = false)
    private String fileName;

    private Long fileSize;

    private String extension;

    private String fileHash;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private FileProcessingStatus fileProcessingStatus;

    @Column(columnDefinition = "TEXT")
    private String errorMessage;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private SourceType sourceType;

    @CreationTimestamp
    @Column(updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;
}
