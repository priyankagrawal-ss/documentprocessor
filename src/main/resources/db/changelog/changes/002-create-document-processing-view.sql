--liquibase formatted sql

--changeset app.user:create-document-processing-view id:002 runOnChange:true
--comment: Creates or replaces the view for live document status tracking with full GroundX and Ingestion status coverage.

CREATE OR REPLACE VIEW document_processing_view AS
SELECT
    -- Generate a stable, unique ID for each row for JPA compatibility.
    ROW_NUMBER() OVER (ORDER BY sub.created_at DESC, sub.file_master_id DESC) AS id,
    sub.file_master_id,
    sub.gx_master_id,
    sub.zip_file_name,
    sub.display_file_name,
    sub.extension,
    sub.file_size,
    sub.gx_bucket_id,
    sub.processing_stage,
    sub.display_status,
    sub.error,
    sub.created_at
FROM (
    ------------------------------------------------------------------
    -- PART 1: Records that have made it to the GroundX stage.
    -- This is the final state for a file.
    ------------------------------------------------------------------
    SELECT
        fm.id                   AS file_master_id,
        gm.id                   AS gx_master_id,
        zm.original_file_name   AS zip_file_name,
        gm.processed_file_name  AS display_file_name,
        gm.extension,
        gm.file_size,
        gm.gx_bucket_id,
        'GroundX Processing'    AS processing_stage,
        CASE gm.gx_status
            WHEN 'COMPLETE'           THEN 'Completed'
            WHEN 'ACTIVE'             THEN 'Active (GroundX)'
            WHEN 'IN_ACTIVE'          THEN 'Inactive (GroundX)'
            WHEN 'SKIPPED'            THEN 'Skipped'
            WHEN 'QUEUED_FOR_UPLOAD'  THEN 'Queued for Upload'
            WHEN 'QUEUED'             THEN 'Queued (GroundX)'
            WHEN 'PROCESSING'         THEN 'Processing (GroundX)'
            WHEN 'ERROR'              THEN 'Error (GroundX)'
            WHEN 'CANCELLED'          THEN 'Cancelled (GroundX)'
            WHEN 'TERMINATED'         THEN 'Terminated (GroundX)'
            ELSE 'Unknown'
        END                     AS display_status,
        gm.error_message        AS error,
        gm.created_at
    FROM
        gx_master gm
    JOIN
        file_master fm ON gm.source_file_id = fm.id
    LEFT JOIN
        zip_master zm ON fm.zip_master_id = zm.id

    UNION ALL

    ------------------------------------------------------------------
    -- PART 2: Records that are still in the Ingestion stage.
    -- We ONLY include files here if they have NOT yet reached the GroundX stage.
    ------------------------------------------------------------------
    SELECT
        fm.id                   AS file_master_id,
        NULL                    AS gx_master_id,
        zm.original_file_name   AS zip_file_name,
        fm.file_name            AS display_file_name,
        fm.extension,
        fm.file_size,
        fm.gx_bucket_id,
        'Ingestion'             AS processing_stage,
        CASE fm.file_processing_status
            WHEN 'QUEUED'       THEN 'Queued (Ingestion)'
            WHEN 'IN_PROGRESS'  THEN 'Processing (Ingestion)'
            WHEN 'FAILED'       THEN 'Error (Ingestion)'
            WHEN 'DUPLICATE'    THEN 'Duplicate'
            WHEN 'IGNORED'      THEN 'Ignored'
            WHEN 'TERMINATED'   THEN 'Terminated (Ingestion)'
			WHEN 'COMPLETE'     THEN 'Completed (Ingestion)'
            ELSE 'Unknown'
        END                     AS display_status,
        fm.error_message        AS error,
        fm.created_at
    FROM
        file_master fm
    LEFT JOIN
        zip_master zm ON fm.zip_master_id = zm.id
    WHERE
        -- This condition is crucial: only show ingestion status for files
        -- that do NOT have a corresponding gx_master record yet.
        NOT EXISTS (
            SELECT 1
            FROM gx_master gm
            WHERE gm.source_file_id = fm.id
        )
        -- And the file has not been successfully completed by the ingestion pipeline
        -- (as it should then have a gx_master record).
      AND fm.file_processing_status != 'COMPLETED'
) AS sub;