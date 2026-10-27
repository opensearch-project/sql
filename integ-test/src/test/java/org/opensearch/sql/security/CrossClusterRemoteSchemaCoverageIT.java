/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.security.SecurityTestBase.STRONG_PASSWORD;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.sql.legacy.TestUtils;

/**
 * More cross-cluster cases on remote-only indices: the Log Explorer request shape (time bounds,
 * which also turns on index pruning), wildcards over rollover indices, type conflicts, nested and
 * multi-fields, the other entry points (SQL, describe, explain), and permissions.
 */
public class CrossClusterRemoteSchemaCoverageIT extends CrossClusterTestBase {

  /** Two remote rollover indices: September (2 docs) and October (2 docs). */
  private static final String ROLLOVER = "ccs_rollover";

  /** Two indices that disagree on the type of "level" (keyword vs text), on both clusters. */
  private static final String CONFLICT = "ccs_conflict";

  /** Local index whose "level" is text, mixed with a remote index whose "level" is keyword. */
  private static final String MIXED_LOCAL = "ccs_mixed_local";

  private static final String NO_MAPPINGS_ROLE = "ccs_no_mappings_role";
  private static final String NO_MAPPINGS_USER = "ccs_no_mappings_user";

  private static final String ROLLOVER_MAPPING =
      """
      {"mappings": {"properties": {
        "@timestamp": {"type": "date"},
        "level": {"type": "keyword"},
        "body": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
        "http": {"properties": {"status_code": {"type": "long"}}}
      }}}
      """;

  @Override
  protected void init() throws Exception {
    super.init();
    createIndex(
        remoteClient(),
        ROLLOVER + "-000001",
        ROLLOVER_MAPPING,
        doc("2026-09-01T10:00:00Z", "info", "ok", 200),
        doc("2026-09-02T10:00:00Z", "error", "disk full", 507));
    createIndex(
        remoteClient(),
        ROLLOVER + "-000002",
        ROLLOVER_MAPPING,
        doc("2026-10-01T10:00:00Z", "error", "gateway down", 503),
        doc("2026-10-02T10:00:00Z", "info", "ok", 200));
    createIndex(
        remoteClient(),
        CONFLICT + "-000001",
        "{\"mappings\": {\"properties\": {\"level\": {\"type\": \"keyword\"}}}}",
        "{\"level\": \"error\"}");
    createIndex(
        remoteClient(),
        CONFLICT + "-000002",
        "{\"mappings\": {\"properties\": {\"level\": {\"type\": \"text\"}}}}",
        "{\"level\": \"info\"}");
    createIndex(
        client(),
        CONFLICT + "-000001",
        "{\"mappings\": {\"properties\": {\"level\": {\"type\": \"keyword\"}}}}",
        "{\"level\": \"error\"}");
    createIndex(
        client(),
        CONFLICT + "-000002",
        "{\"mappings\": {\"properties\": {\"level\": {\"type\": \"text\"}}}}",
        "{\"level\": \"info\"}");
    createIndex(
        client(),
        MIXED_LOCAL,
        "{\"mappings\": {\"properties\": {\"level\": {\"type\": \"text\"}}}}",
        "{\"level\": \"warn\"}");
  }

  @Test
  public void testLogExplorerRequestOverRemoteRolloverIndices() throws IOException {
    JSONObject result =
        executeWithTimeBounds(
            String.format(
                "source=%s:%s-* | where `@timestamp` >= '2026-09-01 00:00:00' and `@timestamp` <="
                    + " '2026-10-31 23:59:59' | stats count() as c",
                REMOTE_CLUSTER, ROLLOVER),
            "2026-09-01 00:00:00",
            "2026-10-31 23:59:59");

    verifyDataRows(result, rows(4));
  }

  /** A time range covering only October lets index pruning drop the September index. */
  @Test
  public void testTimeBoundsNarrowTheRemoteIndices() throws IOException {
    JSONObject result =
        executeWithTimeBounds(
            String.format(
                "source=%s:%s-* | where `@timestamp` >= '2026-10-01 00:00:00' and `@timestamp` <="
                    + " '2026-10-31 23:59:59' | sort `@timestamp` | fields body",
                REMOTE_CLUSTER, ROLLOVER),
            "2026-10-01 00:00:00",
            "2026-10-31 23:59:59");

    verifyDataRows(result, rows("gateway down"), rows("ok"));
  }

  @Test
  public void testWildcardAcrossRemoteRolloverIndices() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s:%s-* | stats count() as c", REMOTE_CLUSTER, ROLLOVER));

    verifyDataRows(result, rows(4));
  }

  @Test
  public void testAllRemoteClustersWildcard() throws IOException {
    JSONObject result = executeQuery(String.format("source=*:%s-* | stats count() as c", ROLLOVER));

    verifyDataRows(result, rows(4));
  }

  /**
   * Indices that disagree on a field's type behave the same remotely as locally (the v2 engine, for
   * one, drops the text index's row when sorting on the conflicted field, on either side).
   */
  @Test
  public void testTypeConflictAcrossRemoteIndicesMatchesLocal() throws IOException {
    for (String tail :
        List.of("| fields level", "| sort level | fields level", "| stats count() as c")) {
      JSONArray local =
          executeQuery(String.format("source=%s-* %s", CONFLICT, tail)).getJSONArray("datarows");
      JSONArray remote =
          executeQuery(String.format("source=%s:%s-* %s", REMOTE_CLUSTER, CONFLICT, tail))
              .getJSONArray("datarows");
      assertEquals(tail, sorted(local), sorted(remote));
    }
  }

  private static List<String> sorted(JSONArray rows) {
    List<String> out = new java.util.ArrayList<>();
    for (int i = 0; i < rows.length(); i++) {
      out.add(rows.get(i).toString());
    }
    java.util.Collections.sort(out);
    return out;
  }

  @Test
  public void testNestedFieldFilterAndSort() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s:%s-* | where `http.status_code` >= 500 | sort - `http.status_code` |"
                    + " fields `http.status_code`, body",
                REMOTE_CLUSTER, ROLLOVER));

    verifyDataRows(result, rows(507, "disk full"), rows(503, "gateway down"));
  }

  /** Grouping by a text field uses its ".keyword" sub-field from the remote mapping. */
  @Test
  public void testGroupByTextFieldWithKeywordSubField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s:%s-* | stats count() as c by body | sort body",
                REMOTE_CLUSTER, ROLLOVER));

    verifyDataRows(result, rows(1, "disk full"), rows(1, "gateway down"), rows(2, "ok"));
  }

  @Test
  public void testMixedLocalAndRemoteWithDifferentFieldTypes() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s,%s:%s-000001 | stats count() as c",
                MIXED_LOCAL, REMOTE_CLUSTER, ROLLOVER));

    verifyDataRows(result, rows(3));
  }

  @Test
  public void testSqlOnRemoteIndex() throws IOException {
    Request request = new Request("POST", "/_plugins/_sql");
    request.setJsonEntity(
        new JSONObject()
            .put(
                "query",
                String.format(
                    "SELECT level FROM `%s:%s-000001` ORDER BY level", REMOTE_CLUSTER, ROLLOVER))
            .toString());
    JSONObject result = toJson(client().performRequest(request));

    verifyDataRows(result, rows("error"), rows("info"));
  }

  @Test
  public void testDescribeRemoteIndex() throws IOException {
    JSONObject result =
        executeQuery(String.format("describe %s:%s-000001", REMOTE_CLUSTER, ROLLOVER));

    JSONArray rows = result.getJSONArray("datarows");
    String columns = rows.toString();
    assertTrue(columns, columns.contains("\"level\""));
    assertTrue(columns, columns.contains("\"http.status_code\""));
  }

  @Test
  public void testExplainRemoteIndex() throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl/_explain");
    request.setJsonEntity(
        new JSONObject()
            .put("query", String.format("source=%s:%s-* | fields body", REMOTE_CLUSTER, ROLLOVER))
            .toString());
    Response response = client().performRequest(request);

    assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /** Without mappings-get on the remote, the error is a permission error, not "no such index". */
  @Test
  public void testMissingMappingsPermissionOnRemoteIsReported() throws IOException {
    createUserOnBothClusters(
        NO_MAPPINGS_ROLE,
        NO_MAPPINGS_USER,
        List.of("indices:data/read/search*", "indices:admin/shards/search_shards"));

    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeQueryAs(
                    String.format("source=%s:%s-* | fields body", REMOTE_CLUSTER, ROLLOVER),
                    NO_MAPPINGS_USER));

    assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
    assertTrue(e.getMessage(), e.getMessage().contains("indices:admin/mappings/get"));
    assertFalse(e.getMessage(), e.getMessage().contains("no such index"));
  }

  private static String doc(String timestamp, String level, String body, int status) {
    return String.format(
        Locale.ROOT,
        "{\"@timestamp\": \"%s\", \"level\": \"%s\", \"body\": \"%s\", \"http\": {\"status_code\":"
            + " %d}}",
        timestamp,
        level,
        body,
        status);
  }

  private static void createIndex(RestClient cluster, String index, String mapping, String... docs)
      throws IOException {
    if (cluster.performRequest(new Request("HEAD", "/" + index)).getStatusLine().getStatusCode()
        == 200) {
      return;
    }
    Request create = new Request("PUT", "/" + index);
    create.setJsonEntity(mapping);
    cluster.performRequest(create);
    StringBuilder bulk = new StringBuilder();
    for (String doc : docs) {
      bulk.append("{\"index\": {}}\n").append(doc).append('\n');
    }
    Request load = new Request("POST", "/" + index + "/_bulk?refresh=true");
    load.setJsonEntity(bulk.toString());
    cluster.performRequest(load);
  }

  /** The request Dashboards sends: query plus request-level time bounds. */
  private JSONObject executeWithTimeBounds(String query, String start, String end)
      throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(
        new JSONObject()
            .put("query", query)
            .put("time_field", "@timestamp")
            .put("start_time", start)
            .put("end_time", end)
            .toString());
    return toJson(client().performRequest(request));
  }

  private void createUserOnBothClusters(String role, String user, List<String> remoteActions)
      throws IOException {
    for (RestClient cluster : List.of(client(), remoteClient())) {
      boolean remote = cluster == remoteClient();
      JSONObject indexPermissions =
          new JSONObject()
              .put("index_patterns", List.of(ROLLOVER + "-*", "*:" + ROLLOVER + "-*"))
              .put(
                  "allowed_actions",
                  remote
                      ? remoteActions
                      : List.of(
                          "indices:data/read/search*",
                          "indices:data/read/field_caps*",
                          "indices:admin/shards/search_shards"));
      put(
          cluster,
          "/_plugins/_security/api/roles/" + role,
          new JSONObject()
              .put("cluster_permissions", List.of("cluster:admin/opensearch/ppl"))
              .put("index_permissions", List.of(indexPermissions))
              .toString());
      put(
          cluster,
          "/_plugins/_security/api/internalusers/" + user,
          String.format(
              Locale.ROOT, "{\"password\": \"%s\", \"backend_roles\": []}", STRONG_PASSWORD));
      put(
          cluster,
          "/_plugins/_security/api/rolesmapping/" + role,
          String.format(Locale.ROOT, "{\"users\": [\"%s\"]}", user));
    }
  }

  private static void put(RestClient cluster, String endpoint, String body) throws IOException {
    Request request = new Request("PUT", endpoint);
    request.setJsonEntity(body);
    int status = cluster.performRequest(request).getStatusLine().getStatusCode();
    assertTrue(endpoint + " returned " + status, status == 200 || status == 201);
  }

  private JSONObject executeQueryAs(String query, String username) throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(new JSONObject().put("query", query).toString());
    RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
    options.addHeader(
        "Authorization",
        "Basic "
            + Base64.getEncoder()
                .encodeToString(
                    (username + ":" + STRONG_PASSWORD).getBytes(StandardCharsets.UTF_8)));
    request.setOptions(options);
    return toJson(client().performRequest(request));
  }

  private static JSONObject toJson(Response response) throws IOException {
    return new JSONObject(TestUtils.getResponseBody(response, true));
  }
}
