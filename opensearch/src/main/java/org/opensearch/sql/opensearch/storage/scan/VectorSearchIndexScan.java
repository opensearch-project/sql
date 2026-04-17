/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.storage.capability.KnnPluginCapability;

/**
 * OpenSearch scan for vector-search relations. Delegates everything to {@link OpenSearchIndexScan}
 * except for {@link #open()}, where it first verifies the k-NN plugin is installed so we fail fast
 * with a clear SQL error before the native request would fail deep in execution. The check is
 * deferred to open() (not applyArguments() or the scan builder) so that analysis-time paths like
 * _explain continue to work on clusters without k-NN.
 */
public class VectorSearchIndexScan extends OpenSearchIndexScan {

  private final KnnPluginCapability knnCapability;

  public VectorSearchIndexScan(
      OpenSearchClient client,
      int maxResponseSize,
      OpenSearchRequest request,
      KnnPluginCapability knnCapability) {
    super(client, maxResponseSize, request);
    this.knnCapability = knnCapability;
  }

  @Override
  public void open() {
    knnCapability.requireInstalled();
    super.open();
  }
}
