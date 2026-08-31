/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.apache.hc.core5.http.HttpHost;
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.Test;
import org.opensearch.client.RestClient;

/**
 * Unit tests for {@link ClusterPlugins}. These exercise the real helper against an in-process
 * {@link HttpServer} that emulates {@code _cat/plugins?h=component}, so nothing is re-implemented
 * in the test. The server follows the same pattern as {@code OtlpHttpTraceReceiver}.
 *
 * <p>Coverage:
 *
 * <ul>
 *   <li>exact component match (positive, collision-negative, unrelated-negative, empty-negative,
 *       multiline positive and multiline collision-negative) so a substring match cannot creep back
 *       in;
 *   <li>probe failures (non-2xx / connection loss) propagate as {@link IOException} instead of
 *       being swallowed into a false "not installed";
 *   <li>the require-or-assume gate: optional+absent skips, required+absent fails, required+present
 *       runs.
 * </ul>
 */
public class ClusterPluginsTests {

  private HttpServer server;
  private RestClient client;

  @After
  public void tearDown() {
    if (client != null) {
      try {
        client.close();
      } catch (IOException ignored) {
        // best effort
      }
      client = null;
    }
    if (server != null) {
      server.stop(0);
      server = null;
    }
    System.clearProperty(ClusterPlugins.REQUIRED_PLUGINS_PROPERTY);
  }

  /**
   * Starts a localhost-only server that answers every request with {@code status} and {@code body}.
   */
  private void startServer(int status, String body) throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    byte[] payload = body.getBytes(StandardCharsets.UTF_8);
    server.createContext(
        "/",
        exchange -> {
          exchange.sendResponseHeaders(status, payload.length == 0 ? -1 : payload.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(payload);
          }
        });
    server.setExecutor(null);
    server.start();
    client =
        RestClient.builder(new HttpHost("http", "127.0.0.1", server.getAddress().getPort()))
            .build();
  }

  // ---- exact component detection -------------------------------------------------------------

  @Test
  public void exactComponentIsDetected() throws IOException {
    startServer(200, "opensearch-security\n");
    assertTrue(ClusterPlugins.isPluginInstalled(client, ClusterPlugins.SECURITY_PLUGIN));
  }

  @Test
  public void securityAnalyticsDoesNotCollideWithSecurity() throws IOException {
    // A cluster that only has security-analytics must NOT be reported as having security.
    startServer(200, "opensearch-security-analytics\n");
    assertFalse(ClusterPlugins.isPluginInstalled(client, ClusterPlugins.SECURITY_PLUGIN));
  }

  @Test
  public void unrelatedComponentsReturnFalse() throws IOException {
    startServer(200, "opensearch-job-scheduler\nopensearch-geospatial\n");
    assertFalse(ClusterPlugins.isPluginInstalled(client, ClusterPlugins.SECURITY_PLUGIN));
  }

  @Test
  public void emptyOutputReturnsFalse() throws IOException {
    startServer(200, "");
    assertFalse(ClusterPlugins.isPluginInstalled(client, ClusterPlugins.SECURITY_PLUGIN));
  }

  @Test
  public void multilineExactMatchIsDetected() throws IOException {
    startServer(
        200,
        "opensearch-job-scheduler\nopensearch-geospatial\nopensearch-security\ntelemetry-otel\n");
    assertTrue(ClusterPlugins.isPluginInstalled(client, ClusterPlugins.SECURITY_PLUGIN));
    assertTrue(ClusterPlugins.isPluginInstalled(client, ClusterPlugins.GEOSPATIAL_PLUGIN));
    assertTrue(ClusterPlugins.isPluginInstalled(client, ClusterPlugins.TELEMETRY_OTEL_PLUGIN));
  }

  @Test
  public void multilineCollisionOnlyReturnsFalse() throws IOException {
    startServer(
        200, "opensearch-security-analytics\nopensearch-job-scheduler\nopensearch-geospatial\n");
    assertFalse(ClusterPlugins.isPluginInstalled(client, ClusterPlugins.SECURITY_PLUGIN));
  }

  // ---- probe failures propagate --------------------------------------------------------------

  @Test
  public void nonSuccessResponsePropagatesIoException() throws IOException {
    // A 5xx from the cluster must surface as an IOException (ResponseException), never a silent
    // "plugin not installed".
    startServer(500, "cluster error");
    assertThrows(
        IOException.class,
        () -> ClusterPlugins.isPluginInstalled(client, ClusterPlugins.SECURITY_PLUGIN));
  }

  @Test
  public void connectionFailurePropagatesIoException() throws IOException {
    // Bind then immediately stop the server so the port is dead: a connection error must propagate.
    startServer(200, "opensearch-security\n");
    server.stop(0);
    server = null;
    assertThrows(
        IOException.class,
        () -> ClusterPlugins.isPluginInstalled(client, ClusterPlugins.SECURITY_PLUGIN));
  }

  // ---- required-plugins property (exact token match) -----------------------------------------

  @Test
  public void isPluginRequiredMatchesExactToken() {
    System.setProperty(
        ClusterPlugins.REQUIRED_PLUGINS_PROPERTY, "telemetry-otel, opensearch-security");
    assertTrue(ClusterPlugins.isPluginRequired(ClusterPlugins.SECURITY_PLUGIN));
    assertTrue(ClusterPlugins.isPluginRequired(ClusterPlugins.TELEMETRY_OTEL_PLUGIN));
    assertFalse(ClusterPlugins.isPluginRequired(ClusterPlugins.GEOSPATIAL_PLUGIN));
  }

  @Test
  public void isPluginRequiredDoesNotCollide() {
    System.setProperty(ClusterPlugins.REQUIRED_PLUGINS_PROPERTY, "opensearch-security-analytics");
    assertFalse(ClusterPlugins.isPluginRequired(ClusterPlugins.SECURITY_PLUGIN));
  }

  // ---- require-or-assume gate ----------------------------------------------------------------

  @Test
  public void optionalAbsentIsSkipped() {
    assertThrows(
        AssumptionViolatedException.class,
        () -> ClusterPlugins.requireOrAssume(false, false, "skip", "fail"));
  }

  @Test
  public void requiredAbsentFails() {
    assertThrows(
        AssertionError.class, () -> ClusterPlugins.requireOrAssume(false, true, "skip", "fail"));
  }

  @Test
  public void requiredPresentRuns() {
    // No exception means the test body would proceed.
    ClusterPlugins.requireOrAssume(true, true, "skip", "fail");
  }

  @Test
  public void optionalPresentRuns() {
    ClusterPlugins.requireOrAssume(true, false, "skip", "fail");
  }

  @Test
  public void requirePluginOrAssumeOptionalAbsentSkips() throws IOException {
    startServer(200, "opensearch-job-scheduler\n");
    assertThrows(
        AssumptionViolatedException.class,
        () ->
            ClusterPlugins.requirePluginOrAssume(
                client, ClusterPlugins.SECURITY_PLUGIN, "skip security"));
  }

  @Test
  public void requirePluginOrAssumeRequiredAbsentFails() throws IOException {
    System.setProperty(ClusterPlugins.REQUIRED_PLUGINS_PROPERTY, ClusterPlugins.SECURITY_PLUGIN);
    startServer(200, "opensearch-job-scheduler\n");
    assertThrows(
        AssertionError.class,
        () ->
            ClusterPlugins.requirePluginOrAssume(
                client, ClusterPlugins.SECURITY_PLUGIN, "skip security"));
  }

  @Test
  public void requirePluginOrAssumeRequiredPresentRuns() throws IOException {
    System.setProperty(ClusterPlugins.REQUIRED_PLUGINS_PROPERTY, ClusterPlugins.SECURITY_PLUGIN);
    startServer(200, "opensearch-security\nopensearch-job-scheduler\n");
    // Present + required => no exception, test proceeds.
    ClusterPlugins.requirePluginOrAssume(client, ClusterPlugins.SECURITY_PLUGIN, "skip security");
  }
}
