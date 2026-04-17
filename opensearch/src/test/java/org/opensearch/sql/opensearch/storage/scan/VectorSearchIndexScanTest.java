/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.storage.capability.KnnPluginCapability;

class VectorSearchIndexScanTest {

  @Test
  void openProbesKnnPluginBeforeFetch() {
    OpenSearchClient client = mock(OpenSearchClient.class);
    OpenSearchRequest request = mock(OpenSearchRequest.class);
    KnnPluginCapability capability = mock(KnnPluginCapability.class);
    doThrow(new ExpressionEvaluationException("k-NN plugin missing"))
        .when(capability)
        .requireInstalled();

    VectorSearchIndexScan scan = new VectorSearchIndexScan(client, 10, request, capability);
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, scan::open);
    assertTrue(ex.getMessage().contains("k-NN plugin"));
    // Capability threw, so the underlying client must not have been touched for this scan.
    verify(client, never()).search(request);
  }
}
