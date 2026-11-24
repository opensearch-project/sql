/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.sql.spark.execution.statestore.OpenSearchStateStoreUtil;
import org.opensearch.sql.spark.execution.statestore.StateStore;

/**
 * Runs the plugin's own access to the async query indices on the secured cluster, with system index
 * protection on. The purge runs as the plugin subject: on the session index, a system index of the
 * plugin, and on the result index, which is not a system index and is allowed only by {@code
 * plugin-additional-permissions.yml}.
 */
public class ResultIndexPluginAccessSecurityIT extends SecurityTestBase {

  private static final String RESULT_INDEX = "query_execution_result_purge_it";
  private static final String DATASOURCE = "result_it_s3";
  private static final String SESSION_INDEX = OpenSearchStateStoreUtil.getIndexName("retention_it");
  private static final String INTERVAL_SETTING =
      "plugins.query.executionengine.spark.auto_index_management.interval";
  private static final String PURGE_FAILED = "delete docs by query fails for result index";
  private static final String PLUGIN_DENIED =
      "and User [name=plugin:org.opensearch.sql.plugin.SQLPlugin";
  private static final long TIMEOUT_MILLIS = 60_000;

  @After
  public void cleanUp() throws IOException {
    updateClusterSettings(new ClusterSetting(TRANSIENT, INTERVAL_SETTING, null));
    deleteIfExists(RESULT_INDEX);
    deleteIfExists(superAdminClient(), SESSION_INDEX);
    deleteIfExists("query_execution_result_" + DATASOURCE);
    Request deleteDataSource = new Request("DELETE", "/_plugins/_query/_datasources/" + DATASOURCE);
    try {
      client().performRequest(deleteDataSource);
    } catch (ResponseException e) {
      // Only the DML test creates it.
      if (e.getResponse().getStatusLine().getStatusCode() != 404) {
        throw e;
      }
    }
  }

  @Test
  public void retentionDeletesExpiredDocsAndKeepsFreshOnes() throws Exception {
    long now = System.currentTimeMillis();
    // The basic auth admin cannot write to a system index, so the session docs are seeded with the
    // admin certificate.
    createSessionIndex();
    Request seedSessions = new Request("POST", "/" + SESSION_INDEX + "/_bulk");
    seedSessions.addParameter("refresh", "true");
    seedSessions.setJsonEntity(
        sessionDoc("expired-session", 1000) + sessionDoc("fresh-session", now));
    assertNoBulkErrors(superAdminClient().performRequest(seedSessions));
    assertEquals(2, count(superAdminClient(), SESSION_INDEX, "{\"match_all\":{}}"));

    createIndexWithMapping(
        RESULT_INDEX,
        "{\"mappings\":{\"properties\":{\"updateTime\":{\"type\":\"date\","
            + "\"format\":\"strict_date_optional_time||epoch_millis\"}}}}");
    Request bulk = new Request("POST", "/" + RESULT_INDEX + "/_bulk");
    bulk.addParameter("refresh", "true");
    bulk.setJsonEntity(
        "{\"index\":{\"_id\":\"expired-1\"}}\n{\"updateTime\":1000}\n"
            + "{\"index\":{\"_id\":\"expired-2\"}}\n{\"updateTime\":2000}\n"
            + String.format(
                Locale.ROOT, "{\"index\":{\"_id\":\"fresh\"}}\n{\"updateTime\":%d}\n", now));
    assertNoBulkErrors(client().performRequest(bulk));
    assertEquals(3, count(RESULT_INDEX, "{\"match_all\":{}}"));

    int problemsBefore = problemLines().size();
    updateClusterSettings(new ClusterSetting(TRANSIENT, INTERVAL_SETTING, "1s"));

    long deadline = System.currentTimeMillis() + TIMEOUT_MILLIS;
    String expired = "{\"range\":{\"updateTime\":{\"lt\":10000}}}";
    String expiredSession = "{\"ids\":{\"values\":[\"expired-session\"]}}";
    while (count(RESULT_INDEX, expired) > 0
        || count(superAdminClient(), SESSION_INDEX, expiredSession) > 0) {
      List<String> problems = newProblemLines(problemsBefore);
      if (!problems.isEmpty()) {
        fail("The purge failed: " + problems);
      }
      if (System.currentTimeMillis() > deadline) {
        fail("The purge did not delete the expired docs in " + TIMEOUT_MILLIS + "ms");
      }
      Thread.sleep(500);
    }
    // The scroll is cleared after the deletes, so let a few more runs finish before reading logs.
    Thread.sleep(3000);

    assertEquals(1, count(RESULT_INDEX, "{\"ids\":{\"values\":[\"fresh\"]}}"));
    assertEquals(1, count(superAdminClient(), SESSION_INDEX, "{\"match_all\":{}}"));
    assertEquals(
        1, count(superAdminClient(), SESSION_INDEX, "{\"ids\":{\"values\":[\"fresh-session\"]}}"));
    assertEquals(List.of(), newProblemLines(problemsBefore));
  }

  @Test
  public void indexDmlResultIsWrittenToTheResultIndex() throws IOException {
    Request createDataSource = new Request("POST", "/_plugins/_query/_datasources");
    createDataSource.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{\"name\":\"%s\",\"connector\":\"S3GLUE\",\"properties\":{"
                + "\"glue.auth.type\":\"iam_role\","
                + "\"glue.auth.role_arn\":\"arn:aws:iam::123456789012:role/it\","
                + "\"glue.indexstore.opensearch.uri\":\"http://localhost:9200\","
                + "\"glue.indexstore.opensearch.auth\":\"noauth\"}}",
            DATASOURCE));
    client().performRequest(createDataSource);

    // No Flint index backs the table, so the handler stores a FAILED result without calling Spark.
    // The first query creates the result index, the second writes into the existing one.
    String resultIndex = "query_execution_result_" + DATASOURCE;
    int problemsBefore = problemLines().size();
    for (int i = 0; i < 2; i++) {
      String queryId = submitDropIndex();
      client().performRequest(new Request("POST", "/" + resultIndex + "/_refresh"));
      assertEquals(
          1,
          count(
              resultIndex, String.format(Locale.ROOT, "{\"term\":{\"queryId\":\"%s\"}}", queryId)));
    }
    assertEquals(List.of(), newProblemLines(problemsBefore));
  }

  private String submitDropIndex() throws IOException {
    Request query = new Request("POST", "/_plugins/_async_query");
    query.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{\"datasource\":\"%s\",\"lang\":\"sql\","
                + "\"query\":\"DROP SKIPPING INDEX ON %s.default.http_logs\"}",
            DATASOURCE,
            DATASOURCE));
    Response response = client().performRequest(query);
    return new JSONObject(getResponseBody(response, true)).getString("queryId");
  }

  private long count(String index, String query) throws IOException {
    return count(client(), index, query);
  }

  private static long count(RestClient client, String index, String query) throws IOException {
    Request request = new Request("POST", "/" + index + "/_count");
    request.setJsonEntity("{\"query\":" + query + "}");
    return new JSONObject(getResponseBody(client.performRequest(request), true)).getLong("count");
  }

  private void deleteIfExists(String index) throws IOException {
    deleteIfExists(client(), index);
  }

  private static void deleteIfExists(RestClient client, String index) throws IOException {
    Request request = new Request("DELETE", "/" + index);
    request.addParameter("ignore_unavailable", "true");
    client.performRequest(request);
  }

  /**
   * Creates the session index with the settings and mapping the plugin's {@link StateStore} uses.
   */
  private void createSessionIndex() throws IOException {
    JSONObject body =
        new JSONObject()
            .put("settings", new JSONObject(loadYaml(StateStore.SETTINGS_FILE_NAME)))
            .put("mappings", new JSONObject(loadYaml(StateStore.MAPPING_FILE_NAME)));
    Request request = new Request("PUT", "/" + SESSION_INDEX);
    request.setJsonEntity(body.toString());
    superAdminClient().performRequest(request);
  }

  private static Map<String, Object> loadYaml(String resource) throws IOException {
    try (InputStream in = StateStore.class.getClassLoader().getResourceAsStream(resource)) {
      assertNotNull("Missing plugin resource " + resource, in);
      return XContentHelper.convertToMap(XContentType.YAML.xContent(), in, true);
    }
  }

  /** A bulk index item for a session doc, which the session purge matches on lastUpdateTime. */
  private static String sessionDoc(String id, long lastUpdateTime) {
    return String.format(
        Locale.ROOT,
        "{\"index\":{\"_id\":\"%s\"}}\n{\"version\":\"1.0\",\"type\":\"session\","
            + "\"sessionType\":\"interactive\",\"sessionId\":\"%s\",\"state\":\"dead\","
            + "\"dataSourceName\":\"retention_it\",\"applicationId\":\"app\",\"jobId\":\"job\","
            + "\"lastUpdateTime\":%d}\n",
        id,
        id,
        lastUpdateTime);
  }

  private static List<String> newProblemLines(int before) throws IOException {
    List<String> lines = problemLines();
    // A rolled log file can shrink the count; report everything rather than miss a line.
    return before <= lines.size() ? lines.subList(before, lines.size()) : lines;
  }

  /**
   * Purge failures and permission denials of the plugin subject in the local test cluster's node
   * logs. A denied clear scroll does not fail the purge, so only the log shows it. Empty when the
   * logs are not local.
   */
  private static List<String> problemLines() throws IOException {
    List<String> lines = new ArrayList<>();
    Path clusters = Paths.get(System.getProperty("project.root", "."), "build", "testclusters");
    if (!Files.isDirectory(clusters)) {
      return lines;
    }
    try (Stream<Path> files = Files.walk(clusters, 3)) {
      for (Path log :
          files
              .filter(p -> p.getParent().getFileName().toString().equals("logs"))
              .filter(p -> p.toString().contains("/integTestWithSecurity-"))
              .filter(p -> p.getFileName().toString().endsWith(".log"))
              .toList()) {
        try (Stream<String> logLines = Files.lines(log)) {
          logLines
              .filter(l -> l.contains(PURGE_FAILED) || l.contains(PLUGIN_DENIED))
              .forEach(lines::add);
        }
      }
    }
    return lines;
  }
}
