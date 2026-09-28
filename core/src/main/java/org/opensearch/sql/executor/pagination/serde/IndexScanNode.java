/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.pagination.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Serializable representation of an {@link
 * org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScan}. The request bytes are the
 * OpenSearch-native serialized form of an {@link
 * org.opensearch.sql.opensearch.request.OpenSearchQueryRequest} in PIT mode.
 */
public record IndexScanNode(
    @JsonProperty("requestBytes") byte[] requestBytes,
    @JsonProperty("maxResponseSize") int maxResponseSize)
    implements SerializablePlanNode {

  @JsonCreator
  public IndexScanNode(
      @JsonProperty("requestBytes") byte[] requestBytes,
      @JsonProperty("maxResponseSize") int maxResponseSize) {
    this.requestBytes = Objects.requireNonNull(requestBytes, "requestBytes").clone();
    this.maxResponseSize = maxResponseSize;
  }

  public byte[] requestBytes() {
    return requestBytes.clone();
  }
}
