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
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.util.ClusterPlugins;

/**
 * The shard-failure warning under the security plugin, for an index-scoped role. Two things only
 * break here: the warning crossing the transport-to-worker handoff (#5739), and judging readability
 * with an API the role allows -- a routing-table read needs cluster:monitor/state, which it lacks.
 *
 * <p>Three indices in one pattern: one in range, one out of range with a field of its own, one
 * whose shard can never be allocated. A bounded query must prune the second and keep the third.
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

  private static boolean usersInitialized = false;

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

  /** Drop it between tests so no other class inherits a red cluster; init() rebuilds it. */
  @After
  public void removeUnreadableIndex() throws IOException {
    if (isIndexExist(client(), UNREADABLE_INDEX)) {
      performRequest(client(), new Request("DELETE", "/" + UNREADABLE_INDEX));
    }
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

  /** The bounds a dashboard sends activate pruning, which is where the warning was lost. */
  @Test
  public void shardFailureWarningSurvivesPruningUnderSecurity() throws IOException {
    JSONObject result =
        executeQueryAsUserWithBounds(
            String.format("source=%s | stats count() as n", PATTERN), USER, "ts", FROM, TO);

    // Only the in-range index contributes; the unreadable index is kept but returns nothing.
    verifyDataRows(result, rows(2));
    assertShardFailureWarning(result);
  }

  /** Keeping the unreadable index must not cost pruning: the out-of-range one still goes. */
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
}
