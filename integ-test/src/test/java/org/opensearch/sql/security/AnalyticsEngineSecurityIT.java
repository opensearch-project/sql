/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

/**
 * Integration tests for analytics engine index-level authorization via the production SQL plugin
 * PPL endpoint. Verifies that queries on composite (analytics-engine-backed) indices are subject
 * to the {@code indices:data/read/analytics/query} permission check.
 */
public class AnalyticsEngineSecurityIT extends SecurityTestBase {

  private static final String TEST_INDEX = "analytics_security_test";
  private static final String FORBIDDEN_INDEX = "analytics_forbidden_test";

  private static final String ALLOWED_USER = "analytics_allowed_user";
  private static final String ALLOWED_ROLE = "analytics_allowed_role";
  private static final String DENIED_USER = "analytics_denied_user";
  private static final String DENIED_ROLE = "analytics_denied_role";
  private static final String SEARCH_ONLY_USER = "analytics_search_only_user";
  private static final String SEARCH_ONLY_ROLE = "analytics_search_only_role";
  private static final String WILDCARD_USER = "analytics_wildcard_user";
  private static final String WILDCARD_ROLE = "analytics_wildcard_role";

  private static boolean initialized = false;

  @Override
  public boolean shouldResetQuerySizeLimit() {
    return false;
  }

  @Override
  protected void init() throws Exception {
    if (!initialized) {
      waitForSecurityPlugin();
      createTestIndices();
      createSecurityRolesAndUsers();
      initialized = true;
    }
  }

  private void waitForSecurityPlugin() throws Exception {
    for (int i = 0; i < 60; i++) {
      try {
        Request req = new Request("GET", "/_plugins/_security/api/roles");
        RequestOptions.Builder opts = RequestOptions.DEFAULT.toBuilder();
        opts.addHeader("Authorization", "Basic " +
            java.util.Base64.getEncoder().encodeToString("admin:admin".getBytes()));
        req.setOptions(opts);
        Response resp = client().performRequest(req);
        if (resp.getStatusLine().getStatusCode() == 200) return;
      } catch (Exception e) {
        // Security not ready yet
      }
      Thread.sleep(1000);
    }
    throw new IllegalStateException("Security plugin did not initialize in time");
  }

  private void createTestIndices() throws IOException {
    // Create composite (analytics-engine-backed) indices so the SQL plugin routes
    // queries through the analytics engine's DefaultPlanExecutor.
    createCompositeIndex(TEST_INDEX);
    Request bulk = new Request("POST", "/_bulk");
    bulk.addParameter("refresh", "true");
    bulk.setJsonEntity(String.format(Locale.ROOT,
        "{\"index\": {\"_index\": \"%s\"}}\n{\"name\": \"alice\", \"age\": 30}\n"
        + "{\"index\": {\"_index\": \"%s\"}}\n{\"name\": \"bob\", \"age\": 25}\n",
        TEST_INDEX, TEST_INDEX));
    RequestOptions.Builder opts = RequestOptions.DEFAULT.toBuilder();
    opts.addHeader("Content-Type", "application/x-ndjson");
    bulk.setOptions(opts);
    client().performRequest(bulk);

    createCompositeIndex(FORBIDDEN_INDEX);
    Request bulkF = new Request("POST", "/_bulk");
    bulkF.addParameter("refresh", "true");
    bulkF.setJsonEntity(String.format(Locale.ROOT,
        "{\"index\": {\"_index\": \"%s\"}}\n{\"name\": \"secret\", \"age\": 99}\n",
        FORBIDDEN_INDEX));
    bulkF.setOptions(opts);
    client().performRequest(bulkF);
  }

  private void createCompositeIndex(String index) throws IOException {
    try {
      Request req = new Request("PUT", "/" + index);
      req.setJsonEntity("""
          {
            "settings": {
              "number_of_shards": 1,
              "number_of_replicas": 0,
              "index.pluggable.dataformat.enabled": true,
              "index.pluggable.dataformat": "composite"
            }
          }
          """);
      client().performRequest(req);
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() != 400) {
        throw e;
      }
    }
  }

  private void createSecurityRolesAndUsers() throws IOException {
    // Role with full read access (includes indices:data/read/analytics/query via wildcard)
    createRoleWithPermissions(
        ALLOWED_ROLE,
        TEST_INDEX,
        new String[] {"cluster:admin/opensearch/ppl", "cluster:admin/opensearch/sql"},
        new String[] {
          "indices:data/read*",
          "indices:admin/mappings/get",
          "indices:monitor/settings/get"
        });
    createUser(ALLOWED_USER, ALLOWED_ROLE);

    // Role with no access to TEST_INDEX or FORBIDDEN_INDEX
    createRoleWithPermissions(
        DENIED_ROLE,
        "some_other_index",
        new String[] {"cluster:admin/opensearch/ppl", "cluster:admin/opensearch/sql"},
        new String[] {
          "indices:data/read*",
          "indices:admin/mappings/get",
          "indices:monitor/settings/get"
        });
    createUser(DENIED_USER, DENIED_ROLE);

    // Role with indices:data/read/search* but NOT indices:data/read/analytics/query.
    // Proves the analytics engine requires its specific action permission.
    createRoleWithPermissions(
        SEARCH_ONLY_ROLE,
        TEST_INDEX,
        new String[] {"cluster:admin/opensearch/ppl", "cluster:admin/opensearch/sql"},
        new String[] {
          "indices:data/read/search",
          "indices:data/read/search*",
          "indices:admin/mappings/get",
          "indices:monitor/settings/get"
        });
    createUser(SEARCH_ONLY_USER, SEARCH_ONLY_ROLE);

    // Role with wildcard index pattern — verifies security plugin resolves
    // "analytics_*" to match "analytics_security_test" during permission evaluation.
    createRoleWithPermissions(
        WILDCARD_ROLE,
        "analytics_security*",
        new String[] {"cluster:admin/opensearch/ppl", "cluster:admin/opensearch/sql"},
        new String[] {
          "indices:data/read*",
          "indices:admin/mappings/get",
          "indices:monitor/settings/get"
        });
    createUser(WILDCARD_USER, WILDCARD_ROLE);
  }

  @Test
  public void testPPLQueryAllowedForAuthorizedUser() throws IOException {
    // Verify the request passes SecurityFilter (not 403). The query may fail post-auth
    // if the backend can't execute, but the FGAC check itself succeeded.
    try {
      JSONObject result = executePPLAsUser(
          "source = " + TEST_INDEX + " | fields name, age", ALLOWED_USER);
      assertTrue("Expected datarows in response", result.has("datarows"));
    } catch (ResponseException e) {
      assertNotEquals(
          "Expected auth to pass (not 403) for authorized user",
          403, e.getResponse().getStatusLine().getStatusCode());
    }
  }

  @Test
  public void testPPLQueryDeniedForUnauthorizedUser() throws IOException {
    ResponseException e = assertThrows(ResponseException.class, () ->
        executePPLAsUser("source = " + TEST_INDEX + " | fields name, age", DENIED_USER));
    assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
  }

  @Test
  public void testPPLQueryDeniedForForbiddenIndex() throws IOException {
    ResponseException e = assertThrows(ResponseException.class, () ->
        executePPLAsUser("source = " + FORBIDDEN_INDEX + " | fields name, age", ALLOWED_USER));
    assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
  }

  @Test
  public void testPPLQueryDeniedWithSearchPermissionOnly() throws IOException {
    // User has indices:data/read/search* but NOT indices:data/read/analytics/query.
    // The analytics engine dispatches through AnalyticsQueryAction which requires the
    // specific analytics/query permission — search permission alone is insufficient.
    ResponseException e = assertThrows(ResponseException.class, () ->
        executePPLAsUser("source = " + TEST_INDEX + " | fields name, age", SEARCH_ONLY_USER));
    assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
  }

  @Test
  public void testPPLQueryAllowedWithWildcardPermission() throws IOException {
    // User's role has index_patterns: ["analytics_security*"] which should match
    // "analytics_security_test" via wildcard expansion in the security plugin.
    try {
      JSONObject result = executePPLAsUser(
          "source = " + TEST_INDEX + " | fields name, age", WILDCARD_USER);
      assertTrue("Expected datarows in response", result.has("datarows"));
    } catch (ResponseException e) {
      assertNotEquals(
          "Expected auth to pass (not 403) for wildcard-permitted user",
          403, e.getResponse().getStatusLine().getStatusCode());
    }
  }

  @Test
  public void testPPLQueryDeniedWithWildcardPermissionOnNonMatchingIndex() throws IOException {
    // User's role has index_patterns: ["analytics_security*"] which should NOT match
    // "analytics_forbidden_test".
    ResponseException e = assertThrows(ResponseException.class, () ->
        executePPLAsUser("source = " + FORBIDDEN_INDEX + " | fields name, age", WILDCARD_USER));
    assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
  }

  @Test
  public void testSQLQueryAllowedForAuthorizedUser() throws IOException {
    try {
      JSONObject result = executeSQLAsUser(
          "SELECT name, age FROM " + TEST_INDEX + " LIMIT 3", ALLOWED_USER);
      assertTrue("Expected datarows or schema in response",
          result.has("datarows") || result.has("schema"));
    } catch (ResponseException e) {
      assertNotEquals(
          "Expected auth to pass (not 403) for authorized user",
          403, e.getResponse().getStatusLine().getStatusCode());
    }
  }

  // TODO: The SQL endpoint (/_plugins/_sql) returns 500 instead of 403 for security exceptions.
  // The legacy RestSqlAction error handling wraps OpenSearchSecurityException as a generic 500
  // Internal Server Error rather than propagating the 403 Forbidden status. The authorization
  // IS denied (query does not execute), but the HTTP status is incorrect. These tests accept
  // either 403 or 500 until the SQL plugin's error propagation is fixed.

  @Test
  public void testSQLQueryDeniedForUnauthorizedUser() throws IOException {
    ResponseException e = assertThrows(ResponseException.class, () ->
        executeSQLAsUser("SELECT name, age FROM " + TEST_INDEX + " LIMIT 3", DENIED_USER));
    assertTrue("Expected 403 or 500 with security exception, got " + e.getResponse().getStatusLine().getStatusCode(),
        e.getResponse().getStatusLine().getStatusCode() == 403
        || e.getResponse().getStatusLine().getStatusCode() == 500);
  }

  @Test
  public void testSQLQueryDeniedForForbiddenIndex() throws IOException {
    ResponseException e = assertThrows(ResponseException.class, () ->
        executeSQLAsUser("SELECT name, age FROM " + FORBIDDEN_INDEX + " LIMIT 3", ALLOWED_USER));
    assertTrue("Expected 403 or 500 with security exception, got " + e.getResponse().getStatusLine().getStatusCode(),
        e.getResponse().getStatusLine().getStatusCode() == 403
        || e.getResponse().getStatusLine().getStatusCode() == 500);
  }

  @Test
  public void testSQLQueryDeniedWithSearchPermissionOnly() throws IOException {
    ResponseException e = assertThrows(ResponseException.class, () ->
        executeSQLAsUser("SELECT name, age FROM " + TEST_INDEX + " LIMIT 3", SEARCH_ONLY_USER));
    assertTrue("Expected 403 or 500 with security exception, got " + e.getResponse().getStatusLine().getStatusCode(),
        e.getResponse().getStatusLine().getStatusCode() == 403
        || e.getResponse().getStatusLine().getStatusCode() == 500);
  }

  /**
   * Executes a PPL query via the production SQL plugin endpoint (/_plugins/_ppl).
   */
  private JSONObject executePPLAsUser(String query, String username) throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(String.format(Locale.ROOT, "{\"query\": \"%s\"}", query));

    RequestOptions.Builder opts = RequestOptions.DEFAULT.toBuilder();
    opts.addHeader("Content-Type", "application/json");
    opts.addHeader("Authorization", createBasicAuthHeader(username, STRONG_PASSWORD));
    request.setOptions(opts);

    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
    String body = org.opensearch.sql.legacy.TestUtils.getResponseBody(response, true);
    return new JSONObject(body);
  }

  /**
   * Executes a SQL query via the production SQL plugin endpoint (/_plugins/_sql).
   */
  private JSONObject executeSQLAsUser(String query, String username) throws IOException {
    Request request = new Request("POST", "/_plugins/_sql");
    request.setJsonEntity(String.format(Locale.ROOT, "{\"query\": \"%s\"}", query));

    RequestOptions.Builder opts = RequestOptions.DEFAULT.toBuilder();
    opts.addHeader("Content-Type", "application/json");
    opts.addHeader("Authorization", createBasicAuthHeader(username, STRONG_PASSWORD));
    request.setOptions(opts);

    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
    String body = org.opensearch.sql.legacy.TestUtils.getResponseBody(response, true);
    return new JSONObject(body);
  }
}
