/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol;

import org.opensearch.jdbc.OpenSearchVersion;

public interface ClusterMetadata {
    String getClusterName();

    String getClusterUUID();

    OpenSearchVersion getVersion();
}
