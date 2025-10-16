package com.eyelevel.documentprocessor.model;

/**
 * Indicates the origin of a file within the system.
 */
public enum SourceType {
    /**
     * The file was directly uploaded by a user as a single file or at the root of a ZIP.
     */
    UPLOADED,

    /**
     * The file was extracted from a container file (e.g., an attachment from a .msg or .zip file).
     */
    EXTRACTED,

    /**
     * The file is the result of a system transformation (e.g., a Word document converted to a PDF).
     * This status is not currently set but is available for future use.
     */
    TRANSFORMED
}