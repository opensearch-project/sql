/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.spi.rest.Redactor;
import org.opensearch.sql.spi.rest.RestEndpointContext;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Proves the built-in {@code /_cluster/health} provider fetches through the transport node client
 * the context carries (the same seam an external provider uses) and returns the response as a
 * single JSON {@code response} column, holding no reference to the sql storage client.
 */
class CoreEndpointsProviderTest {

  @Test
  void clusterHealthFetchesViaContextNodeClient() throws IOException {
    ClusterHealthResponse response = mock(ClusterHealthResponse.class);
    when(response.toXContent(any(XContentBuilder.class), any()))
        .thenAnswer(
            invocation -> {
              XContentBuilder builder = invocation.getArgument(0);
              return builder
                  .startObject()
                  .field("status", "green")
                  .field("number_of_nodes", 3)
                  .endObject();
            });

    NodeClient nodeClient = mock(NodeClient.class, RETURNS_DEEP_STUBS);
    when(nodeClient.admin().cluster().health(any(ClusterHealthRequest.class)).actionGet())
        .thenReturn(response);

    RestEndpointRegistry registry = new RestEndpointRegistry(List.of(new CoreEndpointsProvider()));
    List<ExprValue> rows =
        registry
            .resolve("/_cluster/health")
            .toRows(RestEndpointContext.of(Map.of(), nodeClient), Redactor.NONE);

    assertEquals(1, rows.size());
    String json = rows.get(0).tupleValue().get("response").stringValue();
    assertTrue(json.contains("\"status\":\"green\""));
    assertTrue(json.contains("\"number_of_nodes\":3"));
  }
}
