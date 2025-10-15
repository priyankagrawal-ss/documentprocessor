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

    /**
     * A reference back to the original FileMaster record that was the source for this GX entry.
     */
    @ManyToOne(fetch = FetchType.LAZY)
    // The "unique = true" attribute has been removed to allow a one-to-many relationship.
    @JoinColumn(name = "source_file_id", nullable = false)
    private FileMaster sourceFile;

    /**
     * The GroundX bucket identifier where this file is located.
     */
    @Column(nullable = false)
    private Integer gxBucketId;

    /**
     * The final S3 key of the processed file that was sent to GX.
     */
    @Column(nullable = false)
    private String fileLocation;

    /**
     * The filename as it is known to the GX system.
     */
    @Column(nullable = false)
    private String processedFileName;

    private Long fileSize;
    private String extension;

    /**
     * The ingestion status of the file within the GroundX system (e.g., QUEUED, PROCESSING, COMPLETE).
     */
    @Enumerated(EnumType.STRING)
    private GxStatus gxStatus;

    /**
     * The unique process ID returned by GX, used for status tracking.
     */
    private UUID gxProcessId;

    /**
     * Stores any error message returned from the GX API if ingestion failed.
     */
    private String errorMessage;

    @CreationTimestamp
    @Column(updatable = false)
    private LocalDateTime createdAt;
}