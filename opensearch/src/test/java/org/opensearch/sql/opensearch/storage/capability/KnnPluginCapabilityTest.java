/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.capability;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.ClusterAdminClient;
import org.opensearch.transport.client.node.NodeClient;

@ExtendWith(MockitoExtension.class)
class KnnPluginCapabilityTest {

  @Mock private OpenSearchClient client;
  @Mock private NodeClient nodeClient;
  @Mock private AdminClient adminClient;
  @Mock private ClusterAdminClient clusterAdminClient;
  @Mock private ActionFuture<NodesInfoResponse> nodesInfoFuture;

  @Test
  void skipsWhenNodeClientAbsent() {
    when(client.getNodeClient()).thenReturn(Optional.empty());
    KnnPluginCapability capability = new KnnPluginCapability(client);
    // No exception — REST-client mode cannot probe; execution-time errors remain the signal.
    assertDoesNotThrow(capability::requireInstalled);
  }

  @Test
  void passesWhenKnnPluginInstalled() {
    stubNodesInfo(pluginInfo("org.opensearch.knn.plugin.KNNPlugin"));
    KnnPluginCapability capability = new KnnPluginCapability(client);
    assertDoesNotThrow(capability::requireInstalled);
  }

  @Test
  void throwsWhenKnnPluginAbsent() {
    stubNodesInfo(pluginInfo("org.opensearch.security.OpenSearchSecurityPlugin"));
    KnnPluginCapability capability = new KnnPluginCapability(client);
    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, capability::requireInstalled);
    assertTrue(
        ex.getMessage().contains("k-NN plugin"),
        "Expected k-NN plugin message, got: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("not installed"),
        "Expected 'not installed' phrasing, got: " + ex.getMessage());
  }

  @Test
  void cachesSuccessfulProbeResult() {
    stubNodesInfo(pluginInfo("org.opensearch.knn.plugin.KNNPlugin"));
    KnnPluginCapability capability = new KnnPluginCapability(client);
    capability.requireInstalled();
    capability.requireInstalled();
    capability.requireInstalled();
    // Probe fires once regardless of how many times requireInstalled() is called.
    verify(clusterAdminClient, times(1)).nodesInfo(any(NodesInfoRequest.class));
  }

  @Test
  void cachesNegativeProbeResult() {
    stubNodesInfo(pluginInfo("org.opensearch.security.OpenSearchSecurityPlugin"));
    KnnPluginCapability capability = new KnnPluginCapability(client);
    assertThrows(ExpressionEvaluationException.class, capability::requireInstalled);
    assertThrows(ExpressionEvaluationException.class, capability::requireInstalled);
    verify(clusterAdminClient, times(1)).nodesInfo(any(NodesInfoRequest.class));
  }

  @Test
  void doesNotCacheOnProbeFailure() {
    when(client.getNodeClient()).thenReturn(Optional.of(nodeClient));
    when(nodeClient.admin()).thenReturn(adminClient);
    when(adminClient.cluster()).thenReturn(clusterAdminClient);
    when(clusterAdminClient.nodesInfo(any(NodesInfoRequest.class))).thenReturn(nodesInfoFuture);
    when(nodesInfoFuture.actionGet()).thenThrow(new RuntimeException("transport error"));

    KnnPluginCapability capability = new KnnPluginCapability(client);
    assertDoesNotThrow(capability::requireInstalled); // probe failed — treat as unknown
    assertDoesNotThrow(capability::requireInstalled);
    // Probe retries on each call after a failure — failures are not cached.
    verify(clusterAdminClient, times(2)).nodesInfo(any(NodesInfoRequest.class));
  }

  private void stubNodesInfo(PluginInfo... plugins) {
    when(client.getNodeClient()).thenReturn(Optional.of(nodeClient));
    when(nodeClient.admin()).thenReturn(adminClient);
    when(adminClient.cluster()).thenReturn(clusterAdminClient);
    when(clusterAdminClient.nodesInfo(any(NodesInfoRequest.class))).thenReturn(nodesInfoFuture);

    NodeInfo nodeInfo = mock(NodeInfo.class);
    PluginsAndModules pam = mock(PluginsAndModules.class);
    when(nodeInfo.getInfo(PluginsAndModules.class)).thenReturn(pam);
    when(pam.getPluginInfos()).thenReturn(List.of(plugins));

    NodesInfoResponse response = mock(NodesInfoResponse.class);
    when(response.getNodes()).thenReturn(List.of(nodeInfo));
    when(nodesInfoFuture.actionGet()).thenReturn(response);
  }

  private PluginInfo pluginInfo(String classname) {
    PluginInfo pluginInfo = mock(PluginInfo.class);
    when(pluginInfo.getClassname()).thenReturn(classname);
    return pluginInfo;
  }
}
