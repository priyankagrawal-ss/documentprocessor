package com.eyelevel.documentprocessor.common.apiclient.model;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * Abstract base class for configuring custom headers for API requests.  Subclasses should extend this class and
 * define the specific headers to be included in the requests.
 */
@Getter
@Setter
public abstract class HeaderConfig {

    private List<Header> headers;

    /**
     * Represents a single header with a name and a value.
     */
    @Getter
    @Setter
    public static class Header {

        private String name;
        private String value;
    }
}
