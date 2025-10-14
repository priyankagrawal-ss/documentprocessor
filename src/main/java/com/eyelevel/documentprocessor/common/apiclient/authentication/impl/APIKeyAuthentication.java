package com.eyelevel.documentprocessor.common.apiclient.authentication.impl;

import com.eyelevel.documentprocessor.common.apiclient.authentication.Authentication;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * Implementation of the {@link Authentication} interface for API key authentication.
 *
 * <p>This class applies API key authentication by adding an API key to the authorization headers.
 */
@RequiredArgsConstructor
@Slf4j
public class APIKeyAuthentication implements Authentication {

    private final String headerName;
    private final String apiKey;

    /**
     * Applies API key authentication by adding the API key to the authorization map.
     *
     * @param authorization A map containing authorization headers and their values. The API key is
     *                      added to this map using the specified header name.
     */
    @Override
    public void applyAuthentication(Map<String, String> authorization) {
        log.debug("Applying API key authentication with headerName: {}", headerName);
        try {
            authorization.put(headerName, apiKey);
            log.trace("Authorization is now: {}", authorization.keySet());
        } catch (Exception e) {
            log.error("Error applying API key authentication", e);
        }
    }
}
