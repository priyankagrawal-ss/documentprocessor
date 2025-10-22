--liquibase formatted sql

--changeset app.user:create-document-processing-view id:002 runOnChange:true
--comment: Creates or replaces the view for live document status tracking.
CREATE OR REPLACE VIEW document_processing_view AS
SELECT
    -- Generate a stable, unique ID for each row for JPA compatibility.
    ROW_NUMBER() OVER (ORDER BY sub.created_at DESC, sub.display_file_name ASC) AS id,
    sub.file_master_id,
    sub.gx_master_id,
    sub.zip_file_name,
    sub.display_file_name,
    sub.extension,
    sub.file_size,
    sub.gx_bucket_id,
    sub.processing_stage, -- This column is still useful for backend filtering
    sub.display_status,   -- This column is now much clearer for the user
    sub.error,
    sub.created_at
FROM (
         -- Part 1: Select all final artifacts from GxMaster.
         SELECT
             fm.id                  AS file_master_id,
             gm.id                  AS gx_master_id,
             zm.original_file_name  AS zip_file_name,
             gm.processed_file_name AS display_file_name,
             gm.extension,
             gm.file_size,
             gm.gx_bucket_id,
             'GroundX Processing'   AS processing_stage,
             CASE
                 WHEN gm.gx_status = 'COMPLETE' THEN 'Completed'
                 WHEN gm.gx_status = 'ACTIVE' THEN 'Active'
                 WHEN gm.gx_status = 'SKIPPED' THEN 'Skipped'
                 WHEN gm.gx_status = 'QUEUED_FOR_UPLOAD' THEN 'Queued (GroundX)'
                 WHEN gm.gx_status IN ('QUEUED', 'PROCESSING') THEN 'Processing (GroundX)'
                 WHEN gm.gx_status = 'ERROR' THEN 'GroundX Failed'
                 ELSE 'Unknown'
                 END                    AS display_status,
             gm.error_message       AS error,
             gm.created_at
         FROM gx_master gm
                  JOIN file_master fm ON gm.source_file_id = fm.id
                  LEFT JOIN zip_master zm ON fm.zip_master_id = zm.id

         UNION ALL

         -- Part 2: Select files from FileMaster that are still pending or failed early.
         SELECT
             fm.id                 AS file_master_id,
             NULL                  AS gx_master_id,
             zm.original_file_name AS zip_file_name,
             fm.file_name          AS display_file_name,
             fm.extension,
             fm.file_size,
             fm.gx_bucket_id,
             'Ingestion'           AS processing_stage,
             CASE
                 -- CHANGED: Make status messages specific to the Ingestion stage
                 WHEN fm.file_processing_status = 'QUEUED' THEN 'Queued (Ingestion)'
                 WHEN fm.file_processing_status = 'IN_PROGRESS' THEN 'Processing (Ingestion)'
                 WHEN fm.file_processing_status = 'FAILED' THEN 'Ingestion Failed'
                 WHEN fm.file_processing_status = 'DUPLICATE' THEN 'Duplicate'
                 WHEN fm.file_processing_status = 'IGNORED' THEN 'Ignored'
                 ELSE 'Unknown'
                 END                   AS display_status,
             fm.error_message      AS error,
             fm.created_at
         FROM file_master fm
                  LEFT JOIN zip_master zm ON fm.zip_master_id = zm.id
         WHERE
             NOT EXISTS (SELECT 1 FROM gx_master gm WHERE gm.source_file_id = fm.id)
           AND fm.file_processing_status != 'COMPLETED'
     ) AS sub;