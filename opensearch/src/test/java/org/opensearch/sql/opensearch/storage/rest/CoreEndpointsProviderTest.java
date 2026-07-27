/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.spi.rest.RestEndpointContext;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Proves the built-in {@code /_cluster/health} provider fetches through the transport node client
 * the context carries (the same seam an external provider uses) and flattens the response to the
 * fixed schema, holding no reference to the sql storage client.
 */
class CoreEndpointsProviderTest {

  @Test
  void clusterHealthFetchesViaContextNodeClient() {
    ClusterHealthResponse response = mock(ClusterHealthResponse.class);
    when(response.getClusterName()).thenReturn("test-cluster");
    when(response.getStatus()).thenReturn(ClusterHealthStatus.GREEN);
    when(response.getNumberOfNodes()).thenReturn(3);
    when(response.getNumberOfDataNodes()).thenReturn(2);
    when(response.getActivePrimaryShards()).thenReturn(5);
    when(response.getActiveShards()).thenReturn(10);
    when(response.getRelocatingShards()).thenReturn(0);
    when(response.getInitializingShards()).thenReturn(1);
    when(response.getUnassignedShards()).thenReturn(4);
    when(response.isTimedOut()).thenReturn(false);

    NodeClient nodeClient = mock(NodeClient.class, RETURNS_DEEP_STUBS);
    when(nodeClient.admin().cluster().health(any(ClusterHealthRequest.class)).actionGet())
        .thenReturn(response);

    RestEndpointRegistry registry = new RestEndpointRegistry(List.of(new CoreEndpointsProvider()));
    List<ExprValue> rows =
        registry
            .resolve("/_cluster/health")
            .toRows(RestEndpointContext.of(Map.of(), nodeClient), new RedactionRegistry());

    assertEquals(1, rows.size());
    Map<String, ExprValue> row = rows.get(0).tupleValue();
    assertEquals("test-cluster", row.get("cluster_name").stringValue());
    assertEquals("green", row.get("status").stringValue());
    assertEquals(3, row.get("number_of_nodes").integerValue());
    assertFalse(row.get("timed_out").booleanValue());
  }
}
