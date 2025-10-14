package com.eyelevel.documentprocessor.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "gx_master")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GxMaster {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "source_file_id", nullable = false)
    private FileMaster sourceFile;

    @Column(nullable = false)
    private Integer gxBucketId;

    @Column(nullable = false)
    private String fileLocation;

    @Column(nullable = false)
    private String processedFileName;

    private Long fileSize;
    private String extension;

    @Enumerated(EnumType.STRING)
    private GxStatus gxStatus;

    private UUID gxProcessId;

    private String errorMessage;

    @CreationTimestamp
    @Column(updatable = false)
    private LocalDateTime createdAt;
}