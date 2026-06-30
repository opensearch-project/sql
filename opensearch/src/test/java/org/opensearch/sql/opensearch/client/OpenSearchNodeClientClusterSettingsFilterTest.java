/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.sql.opensearch.storage.rest.RestSettingsFilterHolder;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Verifies the in-cluster {@code rest '/_cluster/settings'} fetcher redacts filtered settings using
 * the node {@link SettingsFilter}, matching the native {@code GET /_cluster/settings} endpoint.
 */
class OpenSearchNodeClientClusterSettingsFilterTest {

  @AfterEach
  void clearHolder() {
    RestSettingsFilterHolder.set(null);
  }

  private OpenSearchNodeClient clientReturning(Settings persistent, Settings transientSettings) {
    NodeClient nodeClient = mock(NodeClient.class, RETURNS_DEEP_STUBS);
    ClusterStateResponse stateResp = mock(ClusterStateResponse.class, RETURNS_DEEP_STUBS);
    when(nodeClient.admin().cluster().state(any(ClusterStateRequest.class)).actionGet())
        .thenReturn(stateResp);
    when(stateResp.getState().metadata().persistentSettings()).thenReturn(persistent);
    when(stateResp.getState().metadata().transientSettings()).thenReturn(transientSettings);
    return new OpenSearchNodeClient(nodeClient);
  }

  @Test
  void clusterSettingsRedactsFilteredKeyWhenFilterPublished() {
    Settings persistent =
        Settings.builder()
            .put("cluster.routing.allocation.enable", "all")
            .put("plugins.secret.token", "supersecret")
            .build();
    OpenSearchNodeClient client = clientReturning(persistent, Settings.EMPTY);

    // Publish a filter that redacts the secret key, exactly as the native endpoint would.
    RestSettingsFilterHolder.set(new SettingsFilter(List.of("plugins.secret.token")));

    List<Map<String, Object>> rows = client.clusterSettings(Map.of());
    Set<String> keys = rows.stream().map(r -> (String) r.get("setting")).collect(toSet());

    assertTrue(keys.contains("cluster.routing.allocation.enable"), "non-secret setting kept");
    assertFalse(keys.contains("plugins.secret.token"), "filtered setting must be redacted");
  }

  @Test
  void clusterSettingsRedactsByGlobPattern() {
    Settings persistent =
        Settings.builder()
            .put("cluster.routing.allocation.enable", "all")
            .put("s3.client.default.secret_key", "AKIAEXAMPLE")
            .build();
    OpenSearchNodeClient client = clientReturning(persistent, Settings.EMPTY);

    RestSettingsFilterHolder.set(new SettingsFilter(List.of("s3.client.*.secret_key")));

    Set<String> keys =
        client.clusterSettings(Map.of()).stream()
            .map(r -> (String) r.get("setting"))
            .collect(toSet());

    assertTrue(keys.contains("cluster.routing.allocation.enable"));
    assertFalse(keys.contains("s3.client.default.secret_key"), "glob-matched secret redacted");
  }

  @Test
  void clusterSettingsReturnsRawWhenNoFilterPublished() {
    Settings persistent = Settings.builder().put("plugins.secret.token", "supersecret").build();
    OpenSearchNodeClient client = clientReturning(persistent, Settings.EMPTY);

    // No filter published: confirms the SettingsFilter is the redaction mechanism (not some other
    // code path). At runtime getRestHandlers always publishes the filter before any query.
    Set<String> keys =
        client.clusterSettings(Map.of()).stream()
            .map(r -> (String) r.get("setting"))
            .collect(toSet());

    assertTrue(keys.contains("plugins.secret.token"));
  }
}
