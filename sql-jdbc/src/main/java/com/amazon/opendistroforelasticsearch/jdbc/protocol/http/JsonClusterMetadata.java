/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.jdbc.protocol.http;

import com.amazon.opendistroforelasticsearch.jdbc.protocol.ClusterMetadata;
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