/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.protocol.http;

import org.opensearch.jdbc.protocol.ClusterMetadata;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonClusterMetadata implements ClusterMetadata {

    @JsonProperty("cluster_name")
    private String clusterName;

    @JsonProperty("cluster_uuid")
    private String clusterUUID;

    @JsonProperty("version")
    private JsonOpenSearchVersion version;

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public String getClusterUUID() {
        return clusterUUID;
    }

    @Override
    public JsonOpenSearchVersion getVersion() {
        return version;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setClusterUUID(String clusterUUID) {
        this.clusterUUID = clusterUUID;
    }

    public void setVersion(JsonOpenSearchVersion version) {
        this.version = version;
    }
}
