/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.capability;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Probes the cluster's Nodes Info API once and caches whether the k-NN plugin is installed, so
 * vectorSearch() fails fast with a clear error when the plugin is absent instead of surfacing a
 * native OpenSearch error deep in execution.
 *
 * <p>The probe requires a {@link NodeClient}. In REST-client mode (standalone SQL service) the node
 * client is absent and the check is skipped — execution-time errors remain the signal there.
 *
 * <p>The check runs lazily at scan open() — i.e. only when a vectorSearch() query is actually
 * executed — so analysis-time paths like _explain and local argument validation keep working on
 * clusters without k-NN.
 */
public class KnnPluginCapability {

  /**
   * Canonical k-NN plugin class. Using the class name (not artifact name) so the check is stable
   * across packaging variants.
   */
  private static final String KNN_PLUGIN_CLASSNAME = "org.opensearch.knn.plugin.KNNPlugin";

  private final OpenSearchClient client;
  private final AtomicReference<Boolean> cached = new AtomicReference<>();

  public KnnPluginCapability(OpenSearchClient client) {
    this.client = client;
  }

  /**
   * Throws {@link ExpressionEvaluationException} with a user-facing message if the k-NN plugin is
   * not installed on any node in the cluster. The result is cached after the first successful
   * probe; probe failures are not cached so the next call retries.
   */
  public void requireInstalled() {
    Boolean hit = cached.get();
    if (hit == null) {
      Optional<Boolean> probed = probe();
      if (probed.isEmpty()) {
        // Probe unavailable (REST-client mode, no NodeClient). Don't block — execution-time
        // errors will surface if k-NN is genuinely missing.
        return;
      }
      hit = probed.get();
      cached.set(hit);
    }
    if (!hit) {
      throw new ExpressionEvaluationException(
          "vectorSearch() requires the k-NN plugin, which is not installed on this cluster."
              + " Install opensearch-knn or use a cluster that has it.");
    }
  }

  private Optional<Boolean> probe() {
    Optional<NodeClient> maybeNode = client.getNodeClient();
    if (maybeNode.isEmpty()) {
      return Optional.empty();
    }
    NodeClient node = maybeNode.get();
    try {
      NodesInfoRequest request = new NodesInfoRequest().clear().addMetric("plugins");
      NodesInfoResponse response = node.admin().cluster().nodesInfo(request).actionGet();
      boolean installed =
          response.getNodes().stream()
              .map(info -> info.getInfo(PluginsAndModules.class))
              .filter(Objects::nonNull)
              .flatMap(p -> p.getPluginInfos().stream())
              .map(PluginInfo::getClassname)
              .anyMatch(KNN_PLUGIN_CLASSNAME::equals);
      return Optional.of(installed);
    } catch (Exception e) {
      // Probe failed (IO error, timeout). Don't cache — let the next call retry.
      return Optional.empty();
    }
  }
}
