/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.sql.spi.rest.ArgSpec;
import org.opensearch.sql.spi.rest.RestEndpointContext;
import org.opensearch.sql.spi.rest.RestEndpointDefinition;
import org.opensearch.sql.spi.rest.RestEndpointProvider;
import org.opensearch.transport.client.node.NodeClient;

/**
 * The built-in {@link RestEndpointProvider}. It ships a single read-only, in-cluster endpoint,
 * {@code /_cluster/health}, expressed as a {@link RestEndpointDefinition}. It is a uniform client
 * of the same SPI an external plugin uses. Additional endpoints are left to follow-up changes.
 *
 * <p>Like any external provider, it fetches at execution time through the transport node client the
 * context carries ({@link RestEndpointContext#client()}), so it holds no reference to the sql
 * storage client and runs under the caller's security thread-context.
 *
 * <p>It returns the full health response in a single JSON {@code response} column; a query extracts
 * the fields it needs with the {@code spath} command or the {@code json_extract} function.
 */
public final class CoreEndpointsProvider implements RestEndpointProvider {

  @Override
  public List<RestEndpointDefinition> getEndpoints() {
    return List.of(
        RestEndpointDefinition.builder()
            .name("/_cluster/health")
            .argSpec(ArgSpec.builder().arg("local", Set.of("true", "false")).build())
            .handler(CoreEndpointsProvider::clusterHealth)
            .build());
  }

  private static List<String> clusterHealth(RestEndpointContext ctx) {
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
    try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
      response.toXContent(builder, ToXContent.EMPTY_PARAMS);
      return List.of(builder.toString());
    } catch (IOException e) {
      throw new IllegalStateException("failed to serialize the /_cluster/health response", e);
    }
  }
}
