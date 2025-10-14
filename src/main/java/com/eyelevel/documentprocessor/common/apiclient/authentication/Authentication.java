package com.eyelevel.documentprocessor.common.apiclient.authentication;

import java.util.Map;

/**
 * Defines the contract for applying authentication to an API request.
 *
 * <p>This interface allows different authentication schemes to be applied to API requests in a
 * consistent manner.
 */
public interface Authentication {

    /**
     * Applies the authentication to the provided authorization map.
     *
     * @param authorization A map containing authorization headers and their values. Implementations
     *                      should add or modify entries in this map to apply the authentication scheme.
     */
    void applyAuthentication(Map<String, String> authorization);
}