/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.security.SecurityTestBase.STRONG_PASSWORD;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import java.util.ArrayList;
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

/**
 * Cross-cluster queries on indices that exist only on the remote cluster. Unlike {@link
 * CrossClusterSearchIT}, nothing here is loaded on the local cluster, so the schema has to be read
 * from the remote (via the remote cluster's GetMappings) rather than from a same-named local index.
 */
public class CrossClusterRemoteSchemaIT extends CrossClusterTestBase {

  /** Exists on the remote cluster only. */
  private static final String REMOTE_ONLY = "ccs_remote_only";

  private static final String REMOTE_ONLY_REMOTE = REMOTE_CLUSTER + ":" + REMOTE_ONLY;

  private static final String FLS_ROLE = "ccs_remote_schema_fls_role";
  private static final String FLS_USER = "ccs_remote_schema_fls_user";

  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    createRemoteOnlyIndex();
  }

  @Test
  public void testRemoteOnlyIndexReturnsRows() throws IOException {
    assertFalse("must not exist locally", indexExists(client(), REMOTE_ONLY));

    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | sort `http.status_code` | fields level, `http.status_code`",
                REMOTE_ONLY_REMOTE));

    verifyDataRows(result, rows("info", 200), rows("error", 503));
  }

  @Test
  public void testRemoteOnlyIndexAggregation() throws IOException {
    JSONObject result =
        executeQuery(String.format("search source=%s | stats count() as c", REMOTE_ONLY_REMOTE));

    verifyDataRows(result, rows(2));
  }

  @Test
  public void testMixedLocalAndRemoteIndices() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s,%s | stats count() as c", TEST_INDEX_BANK, REMOTE_ONLY_REMOTE));

    // 7 bank documents locally plus 2 remote-only documents.
    verifyDataRows(result, rows(9));
  }

  @Test
  public void testMissingRemoteIndexIsIndexNotFound() {
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () -> executeQuery(String.format("search source=%s:does_not_exist_*", REMOTE_CLUSTER)));

    assertEquals(404, e.getResponse().getStatusLine().getStatusCode());
    assertTrue(e.getMessage(), e.getMessage().contains("no such index"));
  }

  @Test
  public void testUnreachableRemoteReportsTheSearchError() throws IOException {
    withUnreachableRemote(
        "ccs_unreachable",
        false,
        () -> {
          ResponseException e =
              assertThrows(
                  ResponseException.class,
                  () -> executeQuery("search source=ccs_unreachable:logs-*"));
          assertTrue(
              e.getMessage(), e.getMessage().contains("Unable to open any proxy connections"));
        });
  }

  /**
   * With skip_unavailable, an unreachable remote is skipped. When it's the only cluster named,
   * there are no fields to query, so the query fails and says which cluster was skipped.
   */
  @Test
  public void testUnreachableRemoteWithSkipUnavailableIsReportedAsSkipped() throws IOException {
    withUnreachableRemote(
        "ccs_skipped",
        true,
        () -> {
          ResponseException e =
              assertThrows(
                  ResponseException.class, () -> executeQuery("search source=ccs_skipped:logs-*"));
          assertEquals(503, e.getResponse().getStatusLine().getStatusCode());
          assertTrue(
              e.getMessage(),
              e.getMessage()
                  .contains(
                      "Remote cluster [ccs_skipped] is unavailable and was skipped"
                          + " (skip_unavailable is true)"));
        });
  }

  /** A skipped remote next to a local index adds nothing; the local index answers alone. */
  @Test
  public void testSkippedRemoteWithLocalIndexReturnsTheLocalRows() throws IOException {
    withUnreachableRemote(
        "ccs_skipped",
        true,
        () ->
            verifyDataRows(
                executeQuery(
                    String.format(
                        "search source=%s,ccs_skipped:logs-* | stats count() as c",
                        TEST_INDEX_BANK)),
                rows(7)));
  }

  /**
   * The remote schema is cached per user: a user whose role hides a field (field-level security)
   * must not be served the full schema cached for an earlier, unrestricted user.
   */
  @Test
  public void testCachedRemoteSchemaIsNotSharedAcrossUsers() throws IOException {
    String query = String.format("search source=%s", REMOTE_ONLY_REMOTE);

    // The admin sees every field; this also fills the cache for the admin.
    assertTrue(schemaOf(executeQuery(query)).contains("body"));

    createFlsUserOnBothClusters(new String[] {"level", "@timestamp"});
    List<String> restricted = schemaOf(executeQueryAs(query, FLS_USER));

    assertTrue(restricted.toString(), restricted.contains("level"));
    assertFalse(restricted.toString(), restricted.contains("body"));
    assertFalse(restricted.toString(), restricted.contains("http.status_code"));
  }

  private void createRemoteOnlyIndex() throws IOException {
    if (indexExists(remoteClient(), REMOTE_ONLY)) {
      return;
    }
    Request create = new Request("PUT", "/" + REMOTE_ONLY);
    create.setJsonEntity(
        """
        {"mappings": {"properties": {
          "@timestamp": {"type": "date"},
          "level": {"type": "keyword"},
          "body": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
          "http": {"properties": {"status_code": {"type": "long"}}}
        }}}
        """);
    remoteClient().performRequest(create);

    Request bulk = new Request("POST", "/" + REMOTE_ONLY + "/_bulk?refresh=true");
    bulk.setJsonEntity(
        """
        {"index": {}}
        {"@timestamp": "2026-09-20T10:00:00Z", "level": "info", "body": "ok", "http": {"status_code": 200}}
        {"index": {}}
        {"@timestamp": "2026-09-21T11:00:00Z", "level": "error", "body": "gateway down", "http": {"status_code": 503}}
        """);
    remoteClient().performRequest(bulk);
  }

  /** Registers a remote alias pointing at a port nothing listens on, runs the check, removes it. */
  private void withUnreachableRemote(String alias, boolean skipUnavailable, Check check)
      throws IOException {
    putRemoteSetting(alias, "\"127.0.0.1:1\"", String.valueOf(skipUnavailable));
    try {
      check.run();
    } finally {
      putRemoteSetting(alias, "null", "null");
    }
  }

  private void putRemoteSetting(String alias, String proxyAddress, String skipUnavailable)
      throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            """
            {"persistent": {
              "cluster.remote.%1$s.mode": %2$s,
              "cluster.remote.%1$s.proxy_address": %3$s,
              "cluster.remote.%1$s.skip_unavailable": %4$s
            }}
            """,
            alias,
            "null".equals(proxyAddress) ? "null" : "\"proxy\"",
            proxyAddress,
            skipUnavailable));
    adminClient().performRequest(request);
  }

  /**
   * Field-level security is evaluated on the remote cluster in CCS, so the role, user, and mapping
   * are created on both clusters. The role also names the cluster-prefixed index, because the local
   * cluster checks some actions (such as point-in-time creation) against "cluster:index".
   */
  private void createFlsUserOnBothClusters(String[] allowedFields) throws IOException {
    StringBuilder fls = new StringBuilder();
    for (String field : allowedFields) {
      fls.append(fls.length() == 0 ? "" : ", ").append('"').append(field).append('"');
    }
    for (RestClient cluster : List.of(client(), remoteClient())) {
      put(
          cluster,
          "/_plugins/_security/api/roles/" + FLS_ROLE,
          String.format(
              Locale.ROOT,
              """
              {
                "cluster_permissions": ["cluster:admin/opensearch/ppl"],
                "index_permissions": [{
                  "index_patterns": ["%1$s", "*:%1$s"],
                  "allowed_actions": [
                    "indices:data/read/search*",
                    "indices:data/read/field_caps*",
                    "indices:admin/shards/search_shards",
                    "indices:admin/mappings/get",
                    "indices:monitor/settings/get",
                    "indices:data/read/point_in_time/create",
                    "indices:data/read/point_in_time/delete"
                  ],
                  "fls": [%2$s]
                }]
              }
              """,
              REMOTE_ONLY,
              fls));
      put(
          cluster,
          "/_plugins/_security/api/internalusers/" + FLS_USER,
          String.format(
              Locale.ROOT, "{\"password\": \"%s\", \"backend_roles\": []}", STRONG_PASSWORD));
      put(
          cluster,
          "/_plugins/_security/api/rolesmapping/" + FLS_ROLE,
          String.format(Locale.ROOT, "{\"users\": [\"%s\"]}", FLS_USER));
    }
  }

  private static void put(RestClient cluster, String endpoint, String body) throws IOException {
    Request request = new Request("PUT", endpoint);
    request.setJsonEntity(body);
    Response response = cluster.performRequest(request);
    int status = response.getStatusLine().getStatusCode();
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
                    (username + ":" + STRONG_PASSWORD)
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    request.setOptions(options);
    Response response = client().performRequest(request);
    return new JSONObject(org.opensearch.sql.legacy.TestUtils.getResponseBody(response, true));
  }

  private static List<String> schemaOf(JSONObject result) {
    JSONArray schema = result.getJSONArray("schema");
    List<String> names = new ArrayList<>();
    for (int i = 0; i < schema.length(); i++) {
      names.add(schema.getJSONObject(i).getString("name"));
    }
    return names;
  }

  private static boolean indexExists(RestClient cluster, String index) throws IOException {
    return cluster.performRequest(new Request("HEAD", "/" + index)).getStatusLine().getStatusCode()
        == 200;
  }

  @FunctionalInterface
  private interface Check {
    void run() throws IOException;
  }
}
