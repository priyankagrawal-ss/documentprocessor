package com.eyelevel.documentprocessor.view;

import com.smartsensesolutions.commons.dao.base.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Immutable;

import java.time.LocalDateTime;

/**
 * Represents a read-only, consolidated view of a document's processing status.
 * <p>
 * This entity is mapped to the {@code document_processing_view} database view, which aggregates
 * data from multiple tables to provide a simplified and performant way to query the state
 * of documents in the processing pipeline. As a view, it is immutable.
 */
@Getter
@Entity
@Immutable
@Table(name = "document_processing_view")
@NoArgsConstructor
@AllArgsConstructor
public class DocumentProcessingView implements BaseEntity {

    /**
     * The unique identifier for the view record.
     */
    @Id
    private Long id;

    /**
     * The foreign key to the {@code FileMaster} entity.
     */
    @Column(name = "file_master_id")
    private Long fileMasterId;

    /**
     * The foreign key to the {@code GxMaster} entity.
     */
    @Column(name = "gx_master_id")
    private Long gxMasterId;

    /**
     * The name of the original zip file, if applicable.
     */
    @Column(name = "zip_file_name")
    private String zipFileName;

    /**
     * The user-friendly display name of the file.
     */
    @Column(name = "display_file_name")
    private String fileName;

    /**
     * The file extension (e.g., "pdf", "docx").
     */
    @Column(name = "extension")
    private String extension;

    /**
     * The size of the file in bytes.
     */
    @Column(name = "file_size")
    private Long fileSize;

    /**
     * The identifier for the client's context bucket, used for grouping uploads.
     */
    @Column(name = "gx_bucket_id")
    private Integer gxBucketId;

    /**
     * The current processing status for display purposes (e.g., "Processing", "Completed", "Failed").
     */
    @Column(name = "display_status")
    private String status;

    @Column(name = "processing_stage")
    private String processingStage;

    /**
     * A description of the error if processing failed.
     */
    @Column(name = "error")
    private String error;

    /**
     * The timestamp when the document record was created.
     */
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;
}