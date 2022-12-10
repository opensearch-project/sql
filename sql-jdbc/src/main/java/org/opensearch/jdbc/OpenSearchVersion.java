/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc;

public interface OpenSearchVersion {
    int getMajor();

    int getMinor();

    int getRevision();

    String getFullVersion();
}
