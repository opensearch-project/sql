/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.util.Capability.TIME_BOUNDS_PRUNING;
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
import org.opensearch.client.ResponseException;
import org.opensearch.sql.util.ClusterPlugins;
import org.opensearch.sql.util.RequiresCapability;

/** Shard-failure warning and index pruning under the security plugin, as an index-scoped role. */
public class ShardFailureWarningSecurityIT extends SecurityTestBase {

  private static final String RECENT_INDEX = "shard_sec_recent";
  private static final String OLD_INDEX = "shard_sec_old";
  private static final String UNREADABLE_INDEX = "shard_sec_unreadable";
  private static final String PATTERN = "shard_sec_*";

  private static final String USER = "shard_sec_user";
  private static final String ROLE = "shard_sec_role";

  /** Covers the recent documents but not the old one. */
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
    setPruning(true);
  }

  /** Drop it between tests so no other class inherits a red cluster; init() rebuilds it. */
  @After
  public void removeUnreadableIndex() throws IOException {
    if (isIndexExist(client(), UNREADABLE_INDEX)) {
      performRequest(client(), new Request("DELETE", "/" + UNREADABLE_INDEX));
    }
    resetPruningToDefault();
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
    // Only this index maps legacy_only, so whether it resolves shows whether pruning ran.
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
    // Pinned to a node that does not exist, so its shard is never allocated.
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

    // Two recent documents plus the old one.
    verifyDataRows(result, rows(3));
    assertShardFailureWarning(result, 3);
  }

  /** Time bounds turn pruning on, which used to drop the unreadable index and its warning. */
  @Test
  @RequiresCapability(TIME_BOUNDS_PRUNING)
  public void shardFailureWarningSurvivesPruningUnderSecurity() throws IOException {
    JSONObject result =
        executeQueryAsUserWithBounds(
            String.format("source=%s | stats count() as n", PATTERN), USER, "ts", FROM, TO);

    // The old index is pruned and the unreadable one kept, so 2 of the 3 shards are searched.
    verifyDataRows(result, rows(2));
    assertShardFailureWarning(result, 2);
  }

  @Test
  @RequiresCapability(TIME_BOUNDS_PRUNING)
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

  private void assertShardFailureWarning(JSONObject result, int totalShards) {
    assertTrue(
        "a result covering only some shards must carry a warning under security",
        result.has("warnings"));
    JSONArray warnings = result.getJSONArray("warnings");
    assertEquals(1, warnings.length());
    JSONObject warning = warnings.getJSONObject(0);
    assertEquals("PARTIAL_RESULT", warning.getString("type"));
    assertEquals(
        String.format(
            Locale.ROOT, "Results are partial: 1 of %d shards did not return data.", totalShards),
        warning.getString("message"));
  }
}
