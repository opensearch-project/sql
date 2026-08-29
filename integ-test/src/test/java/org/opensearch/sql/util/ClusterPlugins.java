/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import java.io.IOException;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;

/**
 * Runtime detection of optional cluster plugins, used to environment-gate integration tests that
 * depend on plugins which are not always installed on the target cluster (for example the plain
 * plugin-enabled {@code integTestRemote} cluster does not bundle security, and a multi-shard
 * external cluster may lack geospatial or telemetry).
 *
 * <p>Mirrors the private {@code isKnnPluginInstalled} probe in {@code VectorSearchIT} /{@code
 * VectorSearchExecutionIT}: it reads {@code /_cat/plugins?h=component} and checks whether the
 * installed component list contains the requested plugin. Tests gate on the result with {@link
 * org.junit.Assume#assumeTrue} so a missing dependency is reported as a skipped assumption rather
 * than a failure, while a cluster that does have the plugin still runs the tests.
 */
public final class ClusterPlugins {

  /** Component name of the OpenSearch security plugin as reported by {@code _cat/plugins}. */
  public static final String SECURITY_PLUGIN = "opensearch-security";

  /** Component name of the OpenSearch geospatial plugin (provides ip2geo / geoip enrichment). */
  public static final String GEOSPATIAL_PLUGIN = "opensearch-geospatial";

  /** Component name of the OpenTelemetry exporter plugin that backs PPL query tracing. */
  public static final String TELEMETRY_OTEL_PLUGIN = "telemetry-otel";

  /**
   * Returns {@code true} when the given plugin component is installed on the cluster the client is
   * pointed at. Any I/O error is treated as "not installed" so the caller skips rather than fails.
   *
   * @param client REST client connected to the cluster under test
   * @param pluginComponentName component name as it appears in {@code _cat/plugins?h=component}
   */
  public static boolean isPluginInstalled(RestClient client, String pluginComponentName) {
    try {
      Response response = client.performRequest(new Request("GET", "/_cat/plugins?h=component"));
      String body = new String(response.getEntity().getContent().readAllBytes());
      return body.contains(pluginComponentName);
    } catch (IOException e) {
      return false;
    }
  }

  private ClusterPlugins() {}
}
