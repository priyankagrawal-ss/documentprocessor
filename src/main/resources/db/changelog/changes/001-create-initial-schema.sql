--liquibase formatted sql

--changeset app.user:create-initial-tables id:001
--comment: Creates the initial tables for the document processing application.

-- Table: processing_job
CREATE TABLE processing_job
(
    id                BIGSERIAL PRIMARY KEY,
    original_filename VARCHAR(255) NOT NULL,
    file_location     VARCHAR(255) NOT NULL,
    status            VARCHAR(255) NOT NULL,
    current_stage     VARCHAR(255),
    error_message     TEXT,
    created_at        TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP WITHOUT TIME ZONE,
    gx_bucket_id      INTEGER,
    skip_gx_process   BOOLEAN      NOT NULL       DEFAULT FALSE,
    remark            TEXT
);

-- Table: zip_master
CREATE TABLE zip_master
(
    id                    BIGSERIAL PRIMARY KEY,
    processing_job_id     BIGINT       NOT NULL UNIQUE,
    gx_bucket_id          INTEGER,
    zip_processing_status VARCHAR(255) NOT NULL,
    original_file_path    VARCHAR(255) NOT NULL,
    original_file_name    VARCHAR(255) NOT NULL,
    file_size             BIGINT,
    error_message         TEXT,
    created_at            TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT fk_zip_master_processing_job FOREIGN KEY (processing_job_id) REFERENCES processing_job (id)
);

-- Table: file_master
CREATE TABLE file_master
(
    id                     BIGSERIAL PRIMARY KEY,
    zip_master_id          BIGINT,
    processing_job_id      BIGINT       NOT NULL,
    gx_bucket_id           INTEGER      NOT NULL,
    duplicate_of_file_id   BIGINT,
    file_location          VARCHAR(255) NOT NULL,
    file_name              VARCHAR(255) NOT NULL,
    file_size              BIGINT,
    extension              VARCHAR(255),
    file_hash              VARCHAR(255),
    file_processing_status VARCHAR(255) NOT NULL,
    error_message          TEXT,
    source_type            VARCHAR(255) NOT NULL,
    created_at             TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at             TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT fk_file_master_zip_master FOREIGN KEY (zip_master_id) REFERENCES zip_master (id),
    CONSTRAINT fk_file_master_processing_job FOREIGN KEY (processing_job_id) REFERENCES processing_job (id)
);

-- Table: gx_master
CREATE TABLE gx_master
(
    id                  BIGSERIAL PRIMARY KEY,
    source_file_id      BIGINT       NOT NULL,
    gx_bucket_id        INTEGER      NOT NULL,
    file_location       VARCHAR(255) NOT NULL,
    processed_file_name VARCHAR(255) NOT NULL,
    file_size           BIGINT,
    extension           VARCHAR(255),
    gx_status           VARCHAR(255),
    gx_process_id       UUID,
    error_message       VARCHAR(255),
    created_at          TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_gx_master_file_master FOREIGN KEY (source_file_id) REFERENCES file_master (id)
);

-- Unique index for active files
CREATE UNIQUE INDEX idx_unique_active_file_hash
    ON file_master (gx_bucket_id, file_hash)
    WHERE (file_processing_status NOT IN ('DUPLICATE', 'IGNORED'));