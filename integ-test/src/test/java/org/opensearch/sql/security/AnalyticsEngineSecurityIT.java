/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.TestUtils;

/**
 * Integration tests for analytics engine index-level authorization via the production SQL plugin
 * PPL endpoint. Verifies that queries on composite (analytics-engine-backed) indices are subject to
 * the {@code indices:data/read/analytics/query} permission check.
 *
 * <p>IMPORTANT: The testClusters config sets {@code cluster.pluggable.dataformat=composite} so that
 * ALL queries route through the analytics engine regardless of how the source is specified
 * (wildcard, alias, comma-separated). Without this, {@code isAnalyticsIndex()} only recognizes
 * concrete index names via per-index metadata lookup.
 */
public class AnalyticsEngineSecurityIT extends SecurityTestBase {

  private static final String TEST_INDEX = "analytics_security_test";
  private static final String FORBIDDEN_INDEX = "analytics_forbidden_test";
  private static final String TEST_INDEX_2 = "analytics_security_extra";
  private static final String TEST_ALIAS = "analytics_alias";
  private static final String MULTI_ALIAS = "analytics_multi_alias";

  private static final String ALLOWED_USER = "analytics_allowed_user";
  private static final String ALLOWED_ROLE = "analytics_allowed_role";
  private static final String DENIED_USER = "analytics_denied_user";
  private static final String DENIED_ROLE = "analytics_denied_role";
  private static final String SEARCH_ONLY_USER = "analytics_search_only_user";
  private static final String SEARCH_ONLY_ROLE = "analytics_search_only_role";
  private static final String WILDCARD_USER = "analytics_wildcard_user";
  private static final String WILDCARD_ROLE = "analytics_wildcard_role";
  private static final String ALIAS_USER = "analytics_alias_user";
  private static final String ALIAS_ROLE = "analytics_alias_role";
  private static final String EXACT_PERM_USER = "analytics_exact_perm_user";
  private static final String EXACT_PERM_ROLE = "analytics_exact_perm_role";
  private static final String NO_CLUSTER_PERM_USER = "analytics_no_cluster_perm_user";
  private static final String NO_CLUSTER_PERM_ROLE = "analytics_no_cluster_perm_role";
  private static final String MULTI_ALIAS_USER = "analytics_multi_alias_user";
  private static final String MULTI_ALIAS_ROLE = "analytics_multi_alias_role";

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
        opts.addHeader(
            "Authorization",
            "Basic " + java.util.Base64.getEncoder().encodeToString("admin:admin".getBytes()));
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
    createCompositeIndex(TEST_INDEX);
    createCompositeIndex(TEST_INDEX_2);
    createCompositeIndex(FORBIDDEN_INDEX);

    Request bulk = new Request("POST", "/_bulk");
    bulk.addParameter("refresh", "true");
    RequestOptions.Builder bulkOpts = RequestOptions.DEFAULT.toBuilder();
    bulkOpts.addHeader("Content-Type", "application/x-ndjson");
    bulk.setOptions(bulkOpts);
    bulk.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{\"index\": {\"_index\": \"%s\"}}\n{\"name\": \"alice\", \"age\": 30}\n"
                + "{\"index\": {\"_index\": \"%s\"}}\n{\"name\": \"bob\", \"age\": 25}\n"
                + "{\"index\": {\"_index\": \"%s\"}}\n{\"name\": \"carol\", \"age\": 28}\n"
                + "{\"index\": {\"_index\": \"%s\"}}\n{\"name\": \"secret\", \"age\": 99}\n",
            TEST_INDEX,
            TEST_INDEX,
            TEST_INDEX_2,
            FORBIDDEN_INDEX));
    client().performRequest(bulk);

    // Single-backing alias pointing to TEST_INDEX
    Request aliasReq = new Request("POST", "/_aliases");
    aliasReq.setJsonEntity(
        String.format(
            Locale.ROOT,
            """
            {"actions": [{"add": {"index": "%s", "alias": "%s"}}]}
            """,
            TEST_INDEX,
            TEST_ALIAS));
    client().performRequest(aliasReq);

    // Multi-backing alias pointing to TEST_INDEX + TEST_INDEX_2
    Request multiAliasReq = new Request("POST", "/_aliases");
    multiAliasReq.setJsonEntity(
        String.format(
            Locale.ROOT,
            """
            {"actions": [
              {"add": {"index": "%s", "alias": "%s"}},
              {"add": {"index": "%s", "alias": "%s"}}
            ]}
            """,
            TEST_INDEX,
            MULTI_ALIAS,
            TEST_INDEX_2,
            MULTI_ALIAS));
    client().performRequest(multiAliasReq);
  }

  private void createCompositeIndex(String index) throws IOException {
    try {
      Request req = new Request("PUT", "/" + index);
      req.setJsonEntity(
          """
          {
            "settings": {
              "number_of_shards": 1,
              "number_of_replicas": 0,
              "index.pluggable.dataformat.enabled": true,
              "index.pluggable.dataformat": "composite",
              "index.composite.primary_data_format": "parquet",
              "index.composite.secondary_data_formats": ["lucene"]
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
          "indices:data/read*", "indices:admin/mappings/get", "indices:monitor/settings/get"
        });
    createUser(ALLOWED_USER, ALLOWED_ROLE);

    // Role with no access to TEST_INDEX or FORBIDDEN_INDEX
    createRoleWithPermissions(
        DENIED_ROLE,
        "some_other_index",
        new String[] {"cluster:admin/opensearch/ppl", "cluster:admin/opensearch/sql"},
        new String[] {
          "indices:data/read*", "indices:admin/mappings/get", "indices:monitor/settings/get"
        });
    createUser(DENIED_USER, DENIED_ROLE);

    // Role with indices:data/read/search* but NOT indices:data/read/analytics/query
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

    // Role with wildcard index pattern matching analytics_security_test and
    // analytics_security_extra
    // but NOT analytics_forbidden_test
    createRoleWithPermissions(
        WILDCARD_ROLE,
        "analytics_security*",
        new String[] {"cluster:admin/opensearch/ppl", "cluster:admin/opensearch/sql"},
        new String[] {
          "indices:data/read*", "indices:admin/mappings/get", "indices:monitor/settings/get"
        });
    createUser(WILDCARD_USER, WILDCARD_ROLE);

    // Role with access only to the single-backing alias
    createRoleWithPermissions(
        ALIAS_ROLE,
        TEST_ALIAS,
        new String[] {"cluster:admin/opensearch/ppl", "cluster:admin/opensearch/sql"},
        new String[] {
          "indices:data/read*", "indices:admin/mappings/get", "indices:monitor/settings/get"
        });
    createUser(ALIAS_USER, ALIAS_ROLE);

    // Role with exactly indices:data/read/analytics/query — minimum sufficient permission
    createRoleWithPermissions(
        EXACT_PERM_ROLE,
        TEST_INDEX,
        new String[] {"cluster:admin/opensearch/ppl", "cluster:admin/opensearch/sql"},
        new String[] {
          "indices:data/read/analytics/query",
          "indices:admin/mappings/get",
          "indices:monitor/settings/get"
        });
    createUser(EXACT_PERM_USER, EXACT_PERM_ROLE);

    // Role with full index permissions but NO cluster-level PPL permission.
    // Proves cluster-level authz still blocks on the AnalyticsQueryAction path.
    createRoleWithPermissions(
        NO_CLUSTER_PERM_ROLE,
        TEST_INDEX,
        new String[] {}, // no cluster permissions
        new String[] {
          "indices:data/read*", "indices:admin/mappings/get", "indices:monitor/settings/get"
        });
    createUser(NO_CLUSTER_PERM_USER, NO_CLUSTER_PERM_ROLE);

    // Role with wildcard pattern covering both backing indices of the multi-alias
    createRoleWithPermissions(
        MULTI_ALIAS_ROLE,
        "analytics_security*",
        new String[] {"cluster:admin/opensearch/ppl", "cluster:admin/opensearch/sql"},
        new String[] {
          "indices:data/read*", "indices:admin/mappings/get", "indices:monitor/settings/get"
        });
    createUser(MULTI_ALIAS_USER, MULTI_ALIAS_ROLE);
  }

  // ===========================================================================================
  // CONCRETE INDEX TESTS — Basic ALLOW/DENY
  // ===========================================================================================

  @Test
  public void testPPLQueryAllowedForAuthorizedUser() throws IOException {
    JSONObject result =
        executePPLAsUser("source = " + TEST_INDEX + " | fields name, age", ALLOWED_USER);
    assertDataRowsPresent(result, "authorized user on concrete index");
    assertContainsName(result, "alice");
  }

  @Test
  public void testPPLQueryDeniedForUnauthorizedUser() throws IOException {
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () -> executePPLAsUser("source = " + TEST_INDEX + " | fields name, age", DENIED_USER));
    assertForbidden(e, "unauthorized user on concrete index");
  }

  @Test
  public void testPPLQueryDeniedForForbiddenIndex() throws IOException {
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executePPLAsUser(
                    "source = " + FORBIDDEN_INDEX + " | fields name, age", ALLOWED_USER));
    assertForbidden(e, "authorized user on forbidden index");
  }

  @Test
  public void testPPLQueryDeniedWithSearchPermissionOnly() throws IOException {
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executePPLAsUser(
                    "source = " + TEST_INDEX + " | fields name, age", SEARCH_ONLY_USER));
    assertForbidden(e, "user with search-only permission", "indices:data/read/analytics/query");
  }

  @Test
  public void testPPLQueryAllowedWithExactAnalyticsQueryPermission() throws IOException {
    JSONObject result =
        executePPLAsUser("source = " + TEST_INDEX + " | fields name, age", EXACT_PERM_USER);
    assertDataRowsPresent(result, "user with exact analytics/query permission");
    assertContainsName(result, "alice");
  }

  // ===========================================================================================
  // WILDCARD INDEX PATTERN TESTS
  // ===========================================================================================

  @Test
  public void testPPLQueryAllowedWithWildcardPermission() throws IOException {
    // WILDCARD_USER role grants analytics_security* — query on concrete index should work
    JSONObject result =
        executePPLAsUser("source = " + TEST_INDEX + " | fields name, age", WILDCARD_USER);
    assertDataRowsPresent(result, "wildcard-permitted user on concrete index");
    assertContainsName(result, "alice");
  }

  @Test
  public void testPPLQueryDeniedWithWildcardPermissionOnNonMatchingIndex() throws IOException {
    // WILDCARD_USER role grants analytics_security* — analytics_forbidden_test doesn't match
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executePPLAsUser(
                    "source = " + FORBIDDEN_INDEX + " | fields name, age", WILDCARD_USER));
    assertForbidden(e, "wildcard user on non-matching index");
  }

  @Test
  public void testPPLQueryWithWildcardSourceAllowed() throws IOException {
    // WILDCARD_USER role grants analytics_security* — wildcard query source=analytics_security*
    // resolves to analytics_security_test + analytics_security_extra, both match role
    JSONObject result =
        executePPLAsUser("source = analytics_security* | fields name, age", WILDCARD_USER);
    assertDataRowsPresent(result, "wildcard query with matching permissions");
    // Should see data from both indices (alice/bob from TEST_INDEX, carol from TEST_INDEX_2)
    JSONArray rows = result.getJSONArray("datarows");
    assertTrue("Expected rows from multiple indices, got " + rows.length(), rows.length() >= 3);
  }

  @Test
  public void testPPLQueryWithWildcardSourceDenied() throws IOException {
    // DENIED_USER has no access to any analytics_* indices
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () -> executePPLAsUser("source = analytics_security* | fields name, age", DENIED_USER));
    assertForbidden(e, "denied user on wildcard source");
  }

  @Test
  public void testPPLQueryWithWildcardSourcePartialAccessDenied() throws IOException {
    // WILDCARD_USER role grants analytics_security* — but query source=analytics_*
    // expands to include analytics_forbidden_test which the role does NOT cover.
    // Must be denied entirely, not partially served.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () -> executePPLAsUser("source = analytics_* | fields name, age", WILDCARD_USER));
    assertForbidden(e, "wildcard user on partially-authorized wildcard expansion");
  }

  // ===========================================================================================
  // ALIAS TESTS
  // ===========================================================================================

  @Test
  public void testPPLQueryAllowedViaAlias() throws IOException {
    // ALIAS_USER role grants analytics_alias — query via alias should succeed
    JSONObject result =
        executePPLAsUser("source = " + TEST_ALIAS + " | fields name, age", ALIAS_USER);
    assertDataRowsPresent(result, "alias-permitted user via alias");
    assertContainsName(result, "alice");
  }

  @Test
  public void testPPLQueryDeniedViaAliasForUnauthorizedUser() throws IOException {
    // DENIED_USER has no access to analytics_alias or the underlying index
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () -> executePPLAsUser("source = " + TEST_ALIAS + " | fields name, age", DENIED_USER));
    assertForbidden(e, "denied user via alias");
  }

  @Test
  public void testPPLQueryMultiBackingAliasAllowed() throws IOException {
    // MULTI_ALIAS_USER role grants analytics_security* which covers both backing indices.
    // Multi-alias resolves to analytics_security_test + analytics_security_extra.
    JSONObject result =
        executePPLAsUser("source = " + MULTI_ALIAS + " | fields name, age", MULTI_ALIAS_USER);
    assertDataRowsPresent(result, "multi-alias user with full backing coverage");
    JSONArray rows = result.getJSONArray("datarows");
    assertTrue("Expected rows from both backing indices, got " + rows.length(), rows.length() >= 3);
  }

  @Test
  public void testPPLQueryMultiBackingAliasDenied() throws IOException {
    // DENIED_USER has no grant on either backing index of the multi-alias
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () -> executePPLAsUser("source = " + MULTI_ALIAS + " | fields name, age", DENIED_USER));
    assertForbidden(e, "denied user on multi-backing alias");
  }

  @Test
  public void testPPLQueryMultiBackingAliasPartialAccessDenied() throws IOException {
    // ALIAS_USER role only grants analytics_alias (single-backing to TEST_INDEX).
    // Multi-alias includes TEST_INDEX_2 which ALIAS_USER does NOT have access to.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () -> executePPLAsUser("source = " + MULTI_ALIAS + " | fields name, age", ALIAS_USER));
    assertForbidden(e, "alias user without coverage of all multi-alias backing indices");
  }

  // ===========================================================================================
  // CLUSTER PERMISSION TESTS
  // ===========================================================================================

  @Test
  public void testPPLQueryDeniedWithoutClusterPermission() throws IOException {
    // NO_CLUSTER_PERM_USER has full index-level read on TEST_INDEX but lacks
    // cluster:admin/opensearch/ppl. Cluster-level authz must block before index check.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executePPLAsUser(
                    "source = " + TEST_INDEX + " | fields name, age", NO_CLUSTER_PERM_USER));
    assertForbidden(e, "user without cluster PPL permission", "cluster:admin/opensearch/ppl");
  }

  // ===========================================================================================
  // SQL TESTS
  // ===========================================================================================

  @Test
  public void testSQLQueryAllowedForAuthorizedUser() throws IOException {
    JSONObject result =
        executeSQLAsUser("SELECT name, age FROM " + TEST_INDEX + " LIMIT 3", ALLOWED_USER);
    assertDataRowsPresent(result, "authorized SQL user");
    assertContainsName(result, "alice");
  }

  @Test
  public void testSQLQueryDeniedForUnauthorizedUser() throws IOException {
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeSQLAsUser("SELECT name, age FROM " + TEST_INDEX + " LIMIT 3", DENIED_USER));
    assertForbidden(e, "unauthorized SQL user");
  }

  @Test
  public void testSQLQueryDeniedForForbiddenIndex() throws IOException {
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeSQLAsUser(
                    "SELECT name, age FROM " + FORBIDDEN_INDEX + " LIMIT 3", ALLOWED_USER));
    assertForbidden(e, "SQL query on forbidden index");
  }

  @Test
  public void testSQLQueryDeniedWithSearchPermissionOnly() throws IOException {
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeSQLAsUser(
                    "SELECT name, age FROM " + TEST_INDEX + " LIMIT 3", SEARCH_ONLY_USER));
    assertForbidden(e, "SQL user with search-only permission");
  }

  // NOTE: There is no testSQLQueryDeniedWithoutClusterPermission test because
  // cluster:admin/opensearch/sql has never been enforced by the SQL plugin. Unlike PPL (which
  // dispatches through TransportPPLQueryAction → cluster:admin/opensearch/ppl), the SQL path
  // is entirely REST-based with no cluster-level transport action. This is pre-existing behavior
  // in both the legacy SQL path and the analytics engine SQL path.

  // ===========================================================================================
  // ROUTING GUARD TESTS — Prove queries execute on analytics engine, not legacy PPL
  // ===========================================================================================

  @Test
  public void testRoutingGuardConcreteIndex() throws IOException {
    // The analytics engine's profile output contains a "plan" object with "query_id" — a field
    // that only appears when the analytics engine handles the query. Legacy PPL profile output
    // uses a different plan structure (PlanNode tree). If routing silently falls back to legacy,
    // this assertion will fail.
    JSONObject result =
        executePPLWithProfileAsUser("source = " + TEST_INDEX + " | fields name, age", ALLOWED_USER);
    assertAnalyticsEngineProfile(result, "routing guard (concrete index)");
    assertDataRowsPresent(result, "routing guard (concrete index)");
  }

  @Test
  public void testRoutingGuardWildcardSource() throws IOException {
    // Wildcard source previously fell back to legacy PPL when cluster.pluggable.dataformat
    // was not set. This test ensures the cluster setting keeps wildcards on analytics path.
    JSONObject result =
        executePPLWithProfileAsUser(
            "source = analytics_security* | fields name, age", WILDCARD_USER);
    assertAnalyticsEngineProfile(result, "routing guard (wildcard source)");
    assertDataRowsPresent(result, "routing guard (wildcard source)");
  }

  @Test
  public void testRoutingGuardAlias() throws IOException {
    // Alias source previously fell back to legacy PPL. This test ensures the cluster setting
    // keeps alias queries on the analytics path.
    JSONObject result =
        executePPLWithProfileAsUser("source = " + TEST_ALIAS + " | fields name, age", ALIAS_USER);
    assertAnalyticsEngineProfile(result, "routing guard (alias)");
    assertDataRowsPresent(result, "routing guard (alias)");
  }

  @Test
  public void testRoutingGuardSQLConcreteIndex() throws IOException {
    // Same as PPL routing guard but for the SQL path (/_plugins/_sql). The SQL analytics router
    // uses a different code path (RestSqlAction → analyticsRouter.apply()) than PPL
    // (TransportPPLQueryAction). Both must route to analytics engine.
    JSONObject result =
        executeSQLWithProfileAsUser(
            "SELECT name, age FROM " + TEST_INDEX + " LIMIT 3", ALLOWED_USER);
    assertAnalyticsEngineProfile(result, "SQL routing guard (concrete index)");
    assertDataRowsPresent(result, "SQL routing guard (concrete index)");
  }

  @Test
  public void testRoutingGuardSQLWildcardSource() throws IOException {
    // SQL doesn't support wildcard table names directly, but we can use the concrete index
    // that matches the wildcard role pattern to verify routing.
    JSONObject result =
        executeSQLWithProfileAsUser(
            "SELECT name, age FROM " + TEST_INDEX + " LIMIT 3", WILDCARD_USER);
    assertAnalyticsEngineProfile(result, "SQL routing guard (wildcard permission)");
    assertDataRowsPresent(result, "SQL routing guard (wildcard permission)");
  }

  @Test
  public void testRoutingGuardSQLAlias() throws IOException {
    // SQL using alias as table name — must route to analytics engine, not legacy.
    JSONObject result =
        executeSQLWithProfileAsUser("SELECT name, age FROM " + TEST_ALIAS + " LIMIT 3", ALIAS_USER);
    assertAnalyticsEngineProfile(result, "SQL routing guard (alias)");
    assertDataRowsPresent(result, "SQL routing guard (alias)");
  }

  // ===========================================================================================
  // COMMA-SEPARATED SOURCE TESTS
  // ===========================================================================================

  @Test
  public void testPPLCommaSourceAllAuthorized() throws IOException {
    // User with analytics_security* role queries two indices they have access to.
    JSONObject result =
        executePPLAsUser(
            "source = " + TEST_INDEX + ", " + TEST_INDEX_2 + " | fields name, age", WILDCARD_USER);
    assertDataRowsPresent(result, "comma source all authorized");
    JSONArray rows = result.getJSONArray("datarows");
    assertTrue("Expected rows from both indices, got " + rows.length(), rows.length() >= 3);
  }

  @Test
  public void testPPLCommaSourcePartiallyAuthorized() throws IOException {
    // User authorized on TEST_INDEX but not FORBIDDEN_INDEX — must deny entirely.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executePPLAsUser(
                    "source = " + TEST_INDEX + ", " + FORBIDDEN_INDEX + " | fields name, age",
                    ALLOWED_USER));
    assertForbidden(e, "comma source partially authorized");
  }

  @Test
  public void testPPLMultiIndexDeniedWithBackticksAuthorizedFirst() throws IOException {
    // Backtick-quoted index names must not bypass FGAC. This is an explicit bypass regression
    // test — the parser must correctly extract both index names even with backtick quoting.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executePPLAsUser(
                    "source = `" + TEST_INDEX + "`, `" + FORBIDDEN_INDEX + "` | fields name, age",
                    ALLOWED_USER));
    assertForbidden(e, "backtick-quoted multi-index with forbidden second");
  }

  @Test
  public void testPPLMultiIndexDeniedWithUnauthorizedFirst() throws IOException {
    // Placing the unauthorized index FIRST must still be denied. Ordering-dependent
    // authorization bypasses are a known attack surface.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executePPLAsUser(
                    "source = " + FORBIDDEN_INDEX + ", " + TEST_INDEX + " | fields name, age",
                    ALLOWED_USER));
    assertForbidden(e, "unauthorized index first in comma-separated source");
  }

  // ===========================================================================================
  // ALIAS GRANT SEMANTICS
  // ===========================================================================================

  @Test
  public void testPPLQueryAllowedViaConcreteIndexForAliasUser() throws IOException {
    // ALIAS_USER's role has index_patterns: ["analytics_alias"]. In OpenSearch's security
    // model, granting access to an alias also implicitly grants access to the underlying
    // concrete index. This verifies the query succeeds via the concrete name.
    JSONObject result =
        executePPLAsUser("source = " + TEST_INDEX + " | fields name, age", ALIAS_USER);
    assertDataRowsPresent(result, "alias user querying concrete index");
    assertContainsName(result, "alice");
  }

  // ===========================================================================================
  // PARSER VALIDATION — Malformed syntax returns 400, not bypass
  // ===========================================================================================

  @Test
  public void testPPLDoubleCommaRejected() throws IOException {
    // Double comma in source must be rejected as malformed syntax (400), not silently
    // passed to the authorization layer where it could produce unexpected behavior.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executePPLAsUser(
                    "source = " + TEST_INDEX + ",," + FORBIDDEN_INDEX + " | fields name, age",
                    ALLOWED_USER));
    assertEquals(
        "Expected 400 for double-comma syntax",
        400,
        e.getResponse().getStatusLine().getStatusCode());
  }

  @Test
  public void testPPLLeadingCommaRejected() throws IOException {
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executePPLAsUser("source = ," + TEST_INDEX + " | fields name, age", ALLOWED_USER));
    assertEquals(
        "Expected 400 for leading comma", 400, e.getResponse().getStatusLine().getStatusCode());
  }

  @Test
  public void testPPLTrailingCommaRejected() throws IOException {
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executePPLAsUser("source = " + TEST_INDEX + ", | fields name, age", ALLOWED_USER));
    assertEquals(
        "Expected 400 for trailing comma", 400, e.getResponse().getStatusLine().getStatusCode());
  }

  @Test
  public void testSQLMultiIndexCommaInFromRejected() throws IOException {
    // SQL FROM "idx1,idx2" — comma inside identifier, should be rejected as syntax error.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeSQLAsUser(
                    "SELECT name, age FROM " + TEST_INDEX + "," + FORBIDDEN_INDEX + " LIMIT 3",
                    ALLOWED_USER));
    assertEquals(
        "Expected 400 for comma in SQL FROM", 400, e.getResponse().getStatusLine().getStatusCode());
  }

  @Test
  public void testSQLMultiIndexCrossJoinRejected() throws IOException {
    // SQL cross join syntax — should not bypass FGAC.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeSQLAsUser(
                    "SELECT a.name FROM " + TEST_INDEX + " a, " + FORBIDDEN_INDEX + " b LIMIT 3",
                    ALLOWED_USER));
    assertEquals(
        "Expected 400 for SQL cross join", 400, e.getResponse().getStatusLine().getStatusCode());
  }

  @Test
  public void testSQLMultiIndexJoinRejected() throws IOException {
    // Explicit JOIN — should not bypass FGAC. May return 403 (security denies access to
    // the forbidden index) or 400 (parser rejects multi-table syntax). Either is acceptable
    // as long as data is never returned.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeSQLAsUser(
                    "SELECT a.name FROM "
                        + TEST_INDEX
                        + " a JOIN "
                        + FORBIDDEN_INDEX
                        + " b ON a.name = b.name LIMIT 3",
                    ALLOWED_USER));
    int status = e.getResponse().getStatusLine().getStatusCode();
    assertTrue(
        "Expected 400 or 403 for SQL explicit JOIN, got " + status, status == 400 || status == 403);
  }

  // ===========================================================================================
  // SQL PARITY TESTS — Mirror PPL wildcard/alias/exact permission tests
  // ===========================================================================================

  @Test
  public void testSQLQueryAllowedWithWildcardPermission() throws IOException {
    // WILDCARD_USER role grants analytics_security* — SQL query on matching concrete index.
    JSONObject result =
        executeSQLAsUser("SELECT name, age FROM " + TEST_INDEX + " LIMIT 3", WILDCARD_USER);
    assertDataRowsPresent(result, "SQL wildcard-permitted user");
    assertContainsName(result, "alice");
  }

  @Test
  public void testSQLQueryDeniedWithWildcardPermissionOnNonMatchingIndex() throws IOException {
    // WILDCARD_USER role grants analytics_security* — analytics_forbidden_test doesn't match.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeSQLAsUser(
                    "SELECT name, age FROM " + FORBIDDEN_INDEX + " LIMIT 3", WILDCARD_USER));
    assertForbidden(e, "SQL wildcard user on non-matching index");
  }

  @Test
  public void testSQLQueryAllowedViaAlias() throws IOException {
    // ALIAS_USER role grants analytics_alias — SQL using alias as table name.
    JSONObject result =
        executeSQLAsUser("SELECT name, age FROM " + TEST_ALIAS + " LIMIT 3", ALIAS_USER);
    assertDataRowsPresent(result, "SQL via alias");
    assertContainsName(result, "alice");
  }

  @Test
  public void testSQLQueryWithExactAnalyticsPermission() throws IOException {
    // EXACT_PERM_USER has only indices:data/read/analytics/query — sufficient for SQL.
    JSONObject result =
        executeSQLAsUser("SELECT name, age FROM " + TEST_INDEX + " LIMIT 3", EXACT_PERM_USER);
    assertDataRowsPresent(result, "SQL with exact analytics/query permission");
  }

  // ===========================================================================================
  // ERROR MESSAGE VALIDATION
  // ===========================================================================================

  @Test
  public void testDeniedResponseContainsActionName() throws IOException {
    // All DENY responses should reference the denied action so users can identify what
    // permission they need. Verifies the error body is actionable.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executePPLAsUser(
                    "source = " + TEST_INDEX + " | fields name, age", SEARCH_ONLY_USER));
    assertForbidden(e, "action name in denied response", "indices:data/read/analytics/query");
  }

  @Test
  public void testDeniedResponseDoesNotLeakInternalDetails() throws IOException {
    // 403 responses should not contain stack traces, internal class names, or node IDs.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () -> executePPLAsUser("source = " + TEST_INDEX + " | fields name, age", DENIED_USER));
    assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
    try {
      String body = TestUtils.getResponseBody(e.getResponse(), true);
      assertFalse(
          "403 response should not contain stack trace, got: " + body,
          body.contains("at org.opensearch") || body.contains(".java:"));
      assertFalse(
          "403 response should not contain node ID, got: " + body,
          body.contains("node_id") || body.contains("nodeId"));
    } catch (IOException ioe) {
      fail("Could not read response body: " + ioe.getMessage());
    }
  }

  // ===========================================================================================
  // HELPER METHODS
  // ===========================================================================================

  /** Executes a PPL query via the production SQL plugin endpoint. Asserts HTTP 200. */
  private JSONObject executePPLAsUser(String query, String username) throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(String.format(Locale.ROOT, "{\"query\": \"%s\"}", query));

    RequestOptions.Builder opts = RequestOptions.DEFAULT.toBuilder();
    opts.addHeader("Content-Type", "application/json");
    opts.addHeader("Authorization", createBasicAuthHeader(username, STRONG_PASSWORD));
    request.setOptions(opts);

    Response response = client().performRequest(request);
    assertEquals(
        "Expected HTTP 200 for query: " + query, 200, response.getStatusLine().getStatusCode());
    String body = TestUtils.getResponseBody(response, true);
    return new JSONObject(body);
  }

  /** Executes a SQL query via the production SQL plugin endpoint. Asserts HTTP 200. */
  private JSONObject executeSQLAsUser(String query, String username) throws IOException {
    Request request = new Request("POST", "/_plugins/_sql");
    request.setJsonEntity(String.format(Locale.ROOT, "{\"query\": \"%s\"}", query));

    RequestOptions.Builder opts = RequestOptions.DEFAULT.toBuilder();
    opts.addHeader("Content-Type", "application/json");
    opts.addHeader("Authorization", createBasicAuthHeader(username, STRONG_PASSWORD));
    request.setOptions(opts);

    Response response = client().performRequest(request);
    assertEquals(
        "Expected HTTP 200 for SQL: " + query, 200, response.getStatusLine().getStatusCode());
    String body = TestUtils.getResponseBody(response, true);
    return new JSONObject(body);
  }

  /**
   * Executes a PPL query with profile=true. The profile response includes a "profile" key that is
   * only present when the analytics engine handles the query. Legacy PPL does not support
   * profile=true in this format, making it a reliable routing indicator.
   */
  private JSONObject executePPLWithProfileAsUser(String query, String username) throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(
        String.format(Locale.ROOT, "{\"query\": \"%s\", \"profile\": true}", query));

    RequestOptions.Builder opts = RequestOptions.DEFAULT.toBuilder();
    opts.addHeader("Content-Type", "application/json");
    opts.addHeader("Authorization", createBasicAuthHeader(username, STRONG_PASSWORD));
    request.setOptions(opts);

    Response response = client().performRequest(request);
    assertEquals(
        "Expected HTTP 200 for profiled query: " + query,
        200,
        response.getStatusLine().getStatusCode());
    String body = TestUtils.getResponseBody(response, true);
    return new JSONObject(body);
  }

  /**
   * Executes a SQL query with profile=true. Same as {@link #executePPLWithProfileAsUser} but for
   * the SQL endpoint.
   */
  private JSONObject executeSQLWithProfileAsUser(String query, String username) throws IOException {
    Request request = new Request("POST", "/_plugins/_sql");
    request.setJsonEntity(
        String.format(Locale.ROOT, "{\"query\": \"%s\", \"profile\": true}", query));

    RequestOptions.Builder opts = RequestOptions.DEFAULT.toBuilder();
    opts.addHeader("Content-Type", "application/json");
    opts.addHeader("Authorization", createBasicAuthHeader(username, STRONG_PASSWORD));
    request.setOptions(opts);

    Response response = client().performRequest(request);
    assertEquals(
        "Expected HTTP 200 for profiled SQL query: " + query,
        200,
        response.getStatusLine().getStatusCode());
    String body = TestUtils.getResponseBody(response, true);
    return new JSONObject(body);
  }

  /**
   * Asserts the response contains analytics-engine-specific profile output. The analytics engine
   * profile includes a "plan" object with "query_id" — a field that is unique to the analytics
   * engine and does not appear in legacy PPL profile output (which uses a PlanNode tree structure).
   */
  private void assertAnalyticsEngineProfile(JSONObject result, String context) {
    assertTrue(
        "Expected 'profile' key in response for " + context + ", got: " + result.keySet(),
        result.has("profile"));
    JSONObject profile = result.getJSONObject("profile");
    assertTrue(
        "Expected 'plan' object in profile for "
            + context
            + " (analytics engine output), got: "
            + profile.keySet(),
        profile.has("plan"));
    JSONObject plan = profile.getJSONObject("plan");
    assertTrue(
        "Expected 'query_id' in profile.plan for "
            + context
            + " (proves analytics engine routing), got: "
            + plan.keySet(),
        plan.has("query_id"));
  }

  /** Asserts the response contains a non-empty datarows array. */
  private void assertDataRowsPresent(JSONObject result, String context) {
    assertTrue(
        "Expected 'datarows' field in response for " + context + ", got: " + result.keySet(),
        result.has("datarows"));
    JSONArray rows = result.getJSONArray("datarows");
    assertTrue(
        "Expected non-empty datarows for " + context + ", got " + rows.length(), rows.length() > 0);
  }

  /** Asserts the response datarows contain a row with the given name value. */
  private void assertContainsName(JSONObject result, String expectedName) {
    JSONArray schema = result.getJSONArray("schema");
    int nameIdx = -1;
    for (int i = 0; i < schema.length(); i++) {
      if ("name".equals(schema.getJSONObject(i).getString("name"))) {
        nameIdx = i;
        break;
      }
    }
    assertTrue("Expected 'name' column in schema", nameIdx >= 0);

    JSONArray rows = result.getJSONArray("datarows");
    boolean found = false;
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      if (expectedName.equals(row.getString(nameIdx))) {
        found = true;
        break;
      }
    }
    assertTrue("Expected to find name='" + expectedName + "' in datarows", found);
  }

  /** Asserts the ResponseException is a 403 Forbidden and body references the expected action. */
  private void assertForbidden(ResponseException e, String context, String expectedAction) {
    assertEquals(
        "Expected 403 for " + context + ", got " + e.getResponse().getStatusLine().getStatusCode(),
        403,
        e.getResponse().getStatusLine().getStatusCode());
    try {
      String body = TestUtils.getResponseBody(e.getResponse(), true);
      assertTrue(
          "Expected error body to reference '"
              + expectedAction
              + "' for "
              + context
              + ", got: "
              + body,
          body.contains(expectedAction) || body.contains("no permissions"));
    } catch (IOException ioe) {
      fail("Could not read response body for " + context + ": " + ioe.getMessage());
    }
  }

  /** Asserts 403 with default check for analytics/query or no permissions. */
  private void assertForbidden(ResponseException e, String context) {
    assertForbidden(e, context, "no permissions");
  }
}
