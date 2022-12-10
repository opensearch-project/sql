/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.auth;

/**
 * Enum representing supported authentication methods
 *
 */
public enum AuthenticationType {

    /**
     * No authentication
     */
    NONE,

    /**
     * HTTP Basic authentication
     */
    BASIC,

    /**
     * AWS Signature V4
     */
    AWS_SIGV4;
}
