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
import org.opensearch.sql.util.ClusterPlugins;

/**
 * The shard-failure warning under the security plugin, for an index-scoped role: the warning must
 * survive the transport-to-worker handoff (#5739). One readable index and one whose shard can never
 * be allocated share a pattern.
 */
public class ShardFailureWarningSecurityIT extends SecurityTestBase {

  private static final String RECENT_INDEX = "shard_sec_recent";
  private static final String UNREADABLE_INDEX = "shard_sec_unreadable";
  private static final String PATTERN = "shard_sec_*";

  private static final String USER = "shard_sec_user";
  private static final String ROLE = "shard_sec_role";

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
    // Requires a node that does not exist, so its shard is never allocated. Created with
    // wait_for_active_shards=0 because it will never have an active shard.
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

    verifyDataRows(result, rows(2));
    assertShardFailureWarning(result);
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
    assertEquals("PARTIAL_RESULT", warning.getString("type"));
    assertEquals(
        "Results are partial: 1 of 2 shards did not return data.", warning.getString("message"));
  }
}
