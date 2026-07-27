/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import static org.opensearch.sql.spi.rest.ColumnType.BOOLEAN;
import static org.opensearch.sql.spi.rest.ColumnType.INTEGER;
import static org.opensearch.sql.spi.rest.ColumnType.STRING;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.sql.spi.rest.ArgSpec;
import org.opensearch.sql.spi.rest.Column;
import org.opensearch.sql.spi.rest.RestEndpointContext;
import org.opensearch.sql.spi.rest.RestEndpointDefinition;
import org.opensearch.sql.spi.rest.RestEndpointProvider;
import org.opensearch.transport.client.node.NodeClient;

/**
 * The built-in {@link RestEndpointProvider}. PR1 ships a single read-only, in-cluster endpoint,
 * {@code /_cluster/health}, expressed as a {@link RestEndpointDefinition}. It is a uniform client
 * of the same SPI an external plugin uses; it holds no privileged position in {@link
 * RestEndpointRegistry}. The network-bearing endpoints ({@code /_cat/*}, {@code /_cluster/state},
 * {@code /_cluster/settings}, {@code /_resolve/index}, proposed {@code /_nodes/info}) are deferred
 * to the AppSec follow-up.
 *
 * <p>Like any external provider, it fetches at execution time through the transport node client the
 * context carries ({@link RestEndpointContext#client()}), so it holds no reference to the sql
 * storage client and runs under the caller's security thread-context.
 */
public final class CoreEndpointsProvider implements RestEndpointProvider {

  @Override
  public List<RestEndpointDefinition> getEndpoints() {
    return List.of(
        // /_cluster/health carries no network identifiers, so every column is RedactionClass.NONE.
        RestEndpointDefinition.builder()
            .name("/_cluster/health")
            .schema(
                List.of(
                    Column.of("cluster_name", STRING),
                    Column.of("status", STRING),
                    Column.of("number_of_nodes", INTEGER),
                    Column.of("number_of_data_nodes", INTEGER),
                    Column.of("active_primary_shards", INTEGER),
                    Column.of("active_shards", INTEGER),
                    Column.of("relocating_shards", INTEGER),
                    Column.of("initializing_shards", INTEGER),
                    Column.of("unassigned_shards", INTEGER),
                    Column.of("timed_out", BOOLEAN)))
            .argSpec(ArgSpec.builder().arg("local", Set.of("true", "false")).build())
            .handler(CoreEndpointsProvider::clusterHealth)
            .build());
  }

  private static List<Map<String, Object>> clusterHealth(RestEndpointContext ctx) {
    NodeClient client = ctx.client();
    if (client == null) {
      throw new IllegalStateException(
          "the /_cluster/health rest endpoint requires an in-cluster node client");
    }
    ClusterHealthRequest request = new ClusterHealthRequest();
    if (Boolean.parseBoolean(ctx.args().get("local"))) {
      request.local(true);
    }
    ClusterHealthResponse response = client.admin().cluster().health(request).actionGet();
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("cluster_name", response.getClusterName());
    row.put("status", response.getStatus().name().toLowerCase(Locale.ROOT));
    row.put("number_of_nodes", response.getNumberOfNodes());
    row.put("number_of_data_nodes", response.getNumberOfDataNodes());
    row.put("active_primary_shards", response.getActivePrimaryShards());
    row.put("active_shards", response.getActiveShards());
    row.put("relocating_shards", response.getRelocatingShards());
    row.put("initializing_shards", response.getInitializingShards());
    row.put("unassigned_shards", response.getUnassignedShards());
    row.put("timed_out", response.isTimedOut());
    return List.of(row);
  }
}
