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

    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "processing_job_id", nullable = false)
    private ProcessingJob processingJob;

    @OneToMany(mappedBy = "zipMaster", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<FileMaster> fileMasters;

    @Column
    private Integer gxBucketId;

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