/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Assume;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;

/**
 * Runtime detection of optional cluster plugins, used to environment-gate integration tests that
 * depend on plugins which are not always installed on the target cluster (for example the plain
 * external {@code integTestRemote} cluster does not bundle security, and a multi-shard external
 * cluster may lack geospatial or telemetry).
 *
 * <p>The probe reads {@code /_cat/plugins?h=component} and matches an installed component by exact
 * line equality (never substring), so {@code opensearch-security} is not falsely reported present
 * on a cluster that only has {@code opensearch-security-analytics}.
 *
 * <p>Gating uses two modes decided by the running Gradle task:
 *
 * <ul>
 *   <li><b>Optional</b> (default, e.g. {@code integTestRemote}): a missing plugin is reported as a
 *       skipped assumption via {@link org.junit.Assume#assumeTrue}. Plain external clusters are the
 *       intended skip environment.
 *   <li><b>Required</b> (dedicated tasks that provision the plugin, e.g. {@code
 *       integTestWithSecurity}, {@code tracingIntegTest}, or the local {@code integTest} that
 *       bundles geospatial): a missing plugin is a hard {@link org.junit.Assert#assertTrue}
 *       failure, so a broken plugin stack never silently vanishes into a green run.
 * </ul>
 *
 * The required set is declared by the task via the {@link #REQUIRED_PLUGINS_PROPERTY} system
 * property; tasks leave it unset to keep the optional (skip-on-absence) behaviour.
 */
public final class ClusterPlugins {

  /** Component name of the OpenSearch security plugin as reported by {@code _cat/plugins}. */
  public static final String SECURITY_PLUGIN = "opensearch-security";

  /** Component name of the OpenSearch geospatial plugin (provides ip2geo / geoip enrichment). */
  public static final String GEOSPATIAL_PLUGIN = "opensearch-geospatial";

  /** Component name of the OpenTelemetry exporter plugin that backs PPL query tracing. */
  public static final String TELEMETRY_OTEL_PLUGIN = "telemetry-otel";

  /**
   * Comma-separated list of plugin component names the current Gradle test task declares mandatory.
   * When a plugin appears here, {@link #requirePluginOrAssume} turns a missing plugin into a hard
   * assertion failure instead of a skipped assumption. Tasks that run against arbitrary external
   * clusters (for example {@code integTestRemote}) leave this unset so absence remains a skip.
   */
  public static final String REQUIRED_PLUGINS_PROPERTY = "tests.required.plugins";

  /**
   * Boolean system property set by dedicated tasks that provision a remote (cross-cluster) cluster.
   * When {@code true}, an absent remote cluster is a hard failure rather than a skipped assumption.
   */
  public static final String REQUIRE_REMOTE_CLUSTER_PROPERTY = "tests.required.remote.cluster";

  /**
   * Name used for {@code REMOTE_CLUSTER} when no remote cluster is configured. It keeps the derived
   * {@code REMOTE_CLUSTER} constant (and the {@code <cluster>:<index>} names built from it)
   * non-null so cross-cluster test classes still load; the {@code @BeforeClass} gate skips or fails
   * the tests before any body that would actually query this non-existent cluster runs.
   */
  public static final String DEFAULT_REMOTE_CLUSTER = "remoteCluster";

  /**
   * Selects the remote cluster name from a comma-separated {@code cluster.names} property value.
   * Returns the first token (leading/trailing whitespace trimmed) that starts with {@code
   * "remote"}, or {@code null} when the value is {@code null}, blank, or contains no such token.
   *
   * <p>Extracted as a pure, side-effect-free function so the selection logic can be unit-tested
   * directly, without mutating the {@code cluster.names} system property, reloading a static
   * initializer, or loading the integration base class (whose {@code OpenSearchTestCase} ancestry
   * runs randomized-runner bootstrap in its static initializer). Trimming ensures a property such
   * as {@code "clusterA, remoteCluster"} still matches the {@code remote*} token despite the space
   * that {@code split(",")} leaves attached.
   *
   * @param clusterNamesProperty raw value of the {@code cluster.names} system property (may be
   *     null)
   * @return the selected remote cluster name, or {@code null} if none is present
   */
  public static String selectRemoteCluster(String clusterNamesProperty) {
    if (clusterNamesProperty == null || clusterNamesProperty.isBlank()) {
      return null;
    }
    for (String cluster : clusterNamesProperty.split(",")) {
      String trimmed = cluster.trim();
      if (trimmed.startsWith("remote")) {
        return trimmed;
      }
    }
    return null;
  }

  /**
   * Returns {@code true} when the given plugin component is installed on the cluster the client is
   * pointed at, matching the {@code _cat/plugins?h=component} output line by line with exact
   * equality. Any I/O or HTTP error (including a non-2xx {@link
   * org.opensearch.client.ResponseException}, which extends {@link IOException}) propagates to the
   * caller so a broken probe fails loudly rather than being misreported as "not installed".
   *
   * @param client REST client connected to the cluster under test
   * @param pluginComponentName component name as it appears in {@code _cat/plugins?h=component}
   * @throws IOException if the probe request fails or returns a non-2xx response
   */
  public static boolean isPluginInstalled(RestClient client, String pluginComponentName)
      throws IOException {
    Response response = client.performRequest(new Request("GET", "/_cat/plugins?h=component"));
    String body;
    try (InputStream content = response.getEntity().getContent()) {
      body = new String(content.readAllBytes(), StandardCharsets.UTF_8);
    }
    for (String line : body.split("\n")) {
      if (line.trim().equals(pluginComponentName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns {@code true} when {@code pluginComponentName} is listed in {@link
   * #REQUIRED_PLUGINS_PROPERTY}. Matching is exact per comma-separated token (whitespace trimmed).
   */
  public static boolean isPluginRequired(String pluginComponentName) {
    String required = System.getProperty(REQUIRED_PLUGINS_PROPERTY, "");
    for (String token : required.split(",")) {
      if (token.trim().equals(pluginComponentName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Gates a test on the presence of {@code pluginComponentName}. If the current task marks the
   * plugin required (see {@link #REQUIRED_PLUGINS_PROPERTY}) an absent plugin fails the test;
   * otherwise it is skipped with {@code skipMessage}. Probe I/O errors propagate.
   *
   * @throws IOException if the underlying probe request fails
   */
  public static void requirePluginOrAssume(
      RestClient client, String pluginComponentName, String skipMessage) throws IOException {
    requireOrAssume(
        isPluginInstalled(client, pluginComponentName),
        isPluginRequired(pluginComponentName),
        skipMessage,
        "Plugin '"
            + pluginComponentName
            + "' is marked required for this task via -D"
            + REQUIRED_PLUGINS_PROPERTY
            + " but is not installed on the target cluster");
  }

  /**
   * Core gate primitive. When {@code required} is {@code true} an absent capability ({@code present
   * == false}) fails via {@link Assert#assertTrue}; otherwise it is skipped via {@link
   * Assume#assumeTrue}. When the capability is present the caller proceeds unchanged in both modes.
   *
   * @param present whether the capability (plugin, remote cluster, ...) is available
   * @param required whether the current task declares the capability mandatory
   * @param skipMessage assumption message used when optional and absent
   * @param failMessage assertion message used when required and absent
   */
  public static void requireOrAssume(
      boolean present, boolean required, String skipMessage, String failMessage) {
    if (required) {
      Assert.assertTrue(failMessage, present);
    } else {
      Assume.assumeTrue(skipMessage, present);
    }
  }

  private ClusterPlugins() {}
}
