package com.eyelevel.documentprocessor.common.apiclient.authentication.impl;

import com.eyelevel.documentprocessor.common.apiclient.authentication.Authentication;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * An implementation of {@link Authentication} that injects a static API key
 * into request headers. This is a common authentication scheme for securing server-to-server API communication.
 */
@Slf4j
public record APIKeyAuthentication(String headerName, String apiKey) implements Authentication {

    /**
     * Applies the API key to the provided authorization map by adding a header with the
     * configured name and key.
     *
     * @param authorization A non-null map of headers to which the API key will be added.
     */
    @Override
    public void applyAuthentication(Map<String, String> authorization) {
        if (authorization == null) {
            log.error("Authorization map cannot be null when applying API key authentication.");
            return;
        }

        log.debug("Applying API key authentication using header: '{}'", headerName);
        try {
            authorization.put(headerName, apiKey);
            log.trace("Applied API key to header. Current authorization keys: {}", authorization.keySet());
        } catch (UnsupportedOperationException e) {
            log.error("Cannot apply API key authentication. The provided authorization map is immutable.", e);
        }
    }
}