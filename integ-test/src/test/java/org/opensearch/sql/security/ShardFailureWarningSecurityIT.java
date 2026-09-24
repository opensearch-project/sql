/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.util.TestUtils.isIndexExist;
import static org.opensearch.sql.util.TestUtils.performRequest;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.util.ClusterPlugins;

/**
 * Runs the shard-failure warning path with the security plugin installed, for a user whose role is
 * index-scoped rather than an admin.
 *
 * <p>Two things can only break under security, and both are guarded here. First, the warning
 * travels the same response channel that #5739 showed the security transport interceptor can drop
 * on the transport-to-worker handoff. Second, deciding whether an index was readable must use an
 * API the role already allows: judging it from the routing table needs {@code
 * cluster:monitor/state}, which an index-scoped role does not carry, so it would silently disable
 * pruning for exactly these users and log a missing-privileges audit event on every query.
 *
 * <p>The fixture holds three indices in one pattern: one in range, one out of range carrying a
 * field of its own, and one whose shard can never be allocated. A query with request-level bounds
 * must therefore prune the out-of-range index (its field stops resolving) while keeping the
 * unreadable one (the response carries a shard-failure warning).
 */
public class ShardFailureWarningSecurityIT extends SecurityTestBase {

  private static final String RECENT_INDEX = "shard_sec_recent";
  private static final String OLD_INDEX = "shard_sec_old";
  private static final String UNREADABLE_INDEX = "shard_sec_unreadable";
  private static final String PATTERN = "shard_sec_*";

  private static final String USER = "shard_sec_user";
  private static final String ROLE = "shard_sec_role";

  /** Wide enough to hold the recent documents and to exclude the old one. */
  private static final String FROM = "2026-09-01 00:00:00";

  private static final String TO = "2026-12-31 00:00:00";

  private boolean usersInitialized = false;

  @Override
  protected void init() throws Exception {
    ClusterPlugins.requirePluginOrAssume(
        client(),
        ClusterPlugins.SECURITY_PLUGIN,
        "opensearch-security plugin not installed on test cluster; skipping FGAC tests");
    super.init();
    enableCalcite();
    if (!usersInitialized) {
      createRoleWithIndexAccess(ROLE, PATTERN);
      createUser(USER, ROLE);
      usersInitialized = true;
    }
    createTestIndices();
  }

  /**
   * Drop the unallocatable index between tests so no other class in the suite inherits a red
   * cluster. {@code init()} runs before every test, so the fixture is rebuilt each time.
   */
  @After
  public void removeUnreadableIndex() throws IOException {
    performRequest(client(), new Request("DELETE", "/" + UNREADABLE_INDEX));
  }

  private void createTestIndices() throws IOException {
    if (!isIndexExist(client(), RECENT_INDEX)) {
      createIndexByRestClient(
          client(), RECENT_INDEX, "{\"mappings\":{\"properties\":{\"ts\":{\"type\":\"date\"}}}}");
      Request bulk = new Request("POST", "/" + RECENT_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n{\"ts\":\"2026-09-10T10:00:00Z\"}\n"
              + "{\"index\":{}}\n{\"ts\":\"2026-09-10T11:00:00Z\"}\n");
      performRequest(client(), bulk);
    }
    // Out of range, and the only index carrying legacy_only: whether pruning ran is visible in
    // whether that field still resolves.
    if (!isIndexExist(client(), OLD_INDEX)) {
      createIndexByRestClient(
          client(),
          OLD_INDEX,
          "{\"mappings\":{\"properties\":{\"ts\":{\"type\":\"date\"},"
              + "\"legacy_only\":{\"type\":\"keyword\"}}}}");
      Request bulk = new Request("POST", "/" + OLD_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n{\"ts\":\"2026-06-01T10:00:00Z\",\"legacy_only\":\"pre-rollover\"}\n");
      performRequest(client(), bulk);
    }
    // Requires a node that does not exist, so its shard is never allocated and no probe can read
    // it. Created with wait_for_active_shards=0 because it will never have an active shard.
    if (!isIndexExist(client(), UNREADABLE_INDEX)) {
      Request create =
          new Request("PUT", "/" + UNREADABLE_INDEX + "?wait_for_active_shards=0&timeout=5s");
      create.setJsonEntity(
          "{\"settings\":{\"index\":{\"number_of_shards\":1,\"number_of_replicas\":0,"
              + "\"routing\":{\"allocation\":{\"require\":{\"_name\":\"no_such_node\"}}}}},"
              + "\"mappings\":{\"properties\":{\"ts\":{\"type\":\"date\"}}}}");
      performRequest(client(), create);
    }
  }

  @Test
  public void shardFailureWarningSurvivesSecurityHandoff() throws IOException {
    JSONObject result =
        executeQueryAsUser(String.format("source=%s | stats count() as n", PATTERN), USER);

    // Both readable indices answer: two recent documents plus the old one.
    verifyDataRows(result, rows(3));
    assertShardFailureWarning(result);
  }

  /**
   * The bounds a dashboard sends activate pruning, which is where the warning was lost: the
   * unreadable index was dropped from the expression, leaving a search that covered every shard it
   * was given and so had nothing to report.
   */
  @Test
  public void shardFailureWarningSurvivesPruningUnderSecurity() throws IOException {
    JSONObject result =
        executeQueryAsUserWithBounds(
            String.format("source=%s | stats count() as n", PATTERN), USER, "ts", FROM, TO);

    // Only the in-range index contributes; the unreadable index is kept but returns nothing.
    verifyDataRows(result, rows(2));
    assertShardFailureWarning(result);
  }

  /**
   * Keeping the unreadable index must not cost pruning for an index-scoped role: the out-of-range
   * index is still dropped, so its field stops resolving. Judging readability from the routing
   * table broke this, because the role carries no {@code cluster:monitor/state} and the denied
   * request made pruning decline altogether.
   */
  @Test
  public void pruningStillNarrowsForAnIndexScopedRole() {
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeQueryAsUserWithBounds(
                    String.format("source=%s | fields legacy_only", PATTERN),
                    USER,
                    "ts",
                    FROM,
                    TO));

    assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
    assertTrue(
        "legacy_only lives only in the out-of-range index, so pruning must remove it: "
            + e.getMessage(),
        e.getMessage().contains("Field [legacy_only] not found."));
  }

  @Test
  public void readableIndexAloneCarriesNoWarningUnderSecurity() throws IOException {
    JSONObject result =
        executeQueryAsUser(String.format("source=%s | stats count() as n", RECENT_INDEX), USER);

    verifyDataRows(result, rows(2));
    assertFalse("a complete result carries no warning", result.has("warnings"));
  }

  private void assertShardFailureWarning(JSONObject result) {
    assertTrue(
        "a result covering only some shards must carry a warning under security",
        result.has("warnings"));
    JSONArray warnings = result.getJSONArray("warnings");
    assertEquals(1, warnings.length());
    JSONObject warning = warnings.getJSONObject(0);
    assertEquals("PARTIAL_RESULT_SHARD_FAILURE", warning.getString("type"));
    assertTrue(
        "warning should say the numbers may be undercounted: " + warning.getString("detail"),
        warning.getString("detail").contains("may be undercounted"));
  }

  /** Like {@link #executeQueryAsUser}, but also sends the per-request time bounds. */
  private JSONObject executeQueryAsUserWithBounds(
      String query, String username, String timeField, String start, String end)
      throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{ \"query\": \"%s\", \"time_field\": \"%s\", \"start_time\": \"%s\","
                + " \"end_time\": \"%s\" }",
            query,
            timeField,
            start,
            end));
    RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
    options.addHeader("Content-Type", "application/json");
    options.addHeader("Authorization", createBasicAuthHeader(username, STRONG_PASSWORD));
    request.setOptions(options);
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
    return new JSONObject(org.opensearch.sql.legacy.TestUtils.getResponseBody(response, true));
  }
}
