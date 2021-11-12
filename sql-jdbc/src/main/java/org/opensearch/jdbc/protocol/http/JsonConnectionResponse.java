/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol.http;

import org.opensearch.jdbc.protocol.ConnectionResponse;
import org.opensearch.jdbc.protocol.ClusterMetadata;

public class JsonConnectionResponse implements ConnectionResponse {
    private ClusterMetadata clusterMetadata;

    public JsonConnectionResponse(ClusterMetadata clusterMetadata) {
        this.clusterMetadata = clusterMetadata;
    }

    @Override
    public ClusterMetadata getClusterMetadata() {
        return clusterMetadata;
    }
}
