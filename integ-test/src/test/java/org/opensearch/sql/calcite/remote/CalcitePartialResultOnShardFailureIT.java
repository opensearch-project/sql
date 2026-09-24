/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * End-to-end tests for surfacing a search that reached only some of its shards.
 *
 * <p>OpenSearch answers with HTTP 200 whenever at least one shard responded ({@code
 * search.default_allow_partial_results} defaults to true), so a result can omit whole shards
 * without any error. The fixture reproduces that deterministically with an index whose shard cannot
 * be allocated: an allocation filter naming a node that does not exist leaves it {@code
 * UNASSIGNED}, so a search over the pattern reports {@code _shards: {total: 2, successful: 1,
 * failed: 0}} -- the node-drop shape, where the missing shard is absent from every counter rather
 * than counted as failed, and a {@code failedShards > 0} check alone would see nothing wrong.
 */
public class CalcitePartialResultOnShardFailureIT extends PPLIntegTestCase {

  private static final String HEALTHY_INDEX = "shard_failure_healthy";
  private static final String UNASSIGNABLE_INDEX = "shard_failure_unassignable";
  private static final String PATTERN = "shard_failure_*";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    createTestIndices();
  }

  /**
   * Drop the unassignable index between tests so no other test class inherits a red cluster. {@code
   * init()} runs before every test, so the fixture is rebuilt each time.
   */
  @After
  public void removeUnassignableIndex() throws IOException {
    performRequest(client(), new Request("DELETE", "/" + UNASSIGNABLE_INDEX));
  }

  private void createTestIndices() throws IOException {
    if (!isIndexExist(client(), HEALTHY_INDEX)) {
      String mapping =
          "{\"settings\":{\"index\":{\"number_of_shards\":1,\"number_of_replicas\":0}},"
              + "\"mappings\":{\"properties\":{\"bytes\":{\"type\":\"long\"},"
              + "\"host\":{\"type\":\"keyword\"}}}}";
      createIndexByRestClient(client(), HEALTHY_INDEX, mapping);
      Request bulk = new Request("POST", "/" + HEALTHY_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n{\"bytes\":500,\"host\":\"a\"}\n"
              + "{\"index\":{}}\n{\"bytes\":900,\"host\":\"a\"}\n"
              + "{\"index\":{}}\n{\"bytes\":1500,\"host\":\"b\"}\n");
      performRequest(client(), bulk);
    }
    if (!isIndexExist(client(), UNASSIGNABLE_INDEX)) {
      // Require a node that does not exist, so the shard can never be allocated. Created with
      // wait_for_active_shards=0 because it will never have an active shard to wait for.
      Request create =
          new Request("PUT", "/" + UNASSIGNABLE_INDEX + "?wait_for_active_shards=0&timeout=5s");
      create.setJsonEntity(
          "{\"settings\":{\"index\":{\"number_of_shards\":1,\"number_of_replicas\":0,"
              + "\"routing\":{\"allocation\":{\"require\":{\"_name\":\"no_such_node\"}}}}},"
              + "\"mappings\":{\"properties\":{\"bytes\":{\"type\":\"long\"},"
              + "\"host\":{\"type\":\"keyword\"}}}}");
      performRequest(client(), create);
    }
  }

  @Test
  public void missingShardAttachesWarningNamingTheCounts() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | stats count() as n", PATTERN));

    // Only the healthy index answered, so its 3 documents are all that is counted.
    verifyDataRows(result, rows(3));

    assertTrue("response should carry a warnings array", result.has("warnings"));
    JSONArray warnings = result.getJSONArray("warnings");
    assertEquals(1, warnings.length());
    JSONObject warning = warnings.getJSONObject(0);
    assertEquals("PARTIAL_RESULT_SHARD_FAILURE", warning.getString("type"));
    assertEquals(
        "Results are partial: 1 of 2 shards did not return data.", warning.getString("message"));
    String detail = warning.getString("detail");
    assertTrue(
        "detail should say the numbers may be undercounted: " + detail,
        detail.contains("may be undercounted"));
    assertTrue(
        "detail should explain that no shard copy was available: " + detail,
        detail.contains("No copy of those shards was available"));
  }

  @Test
  public void missingShardIsReportedForRowsAsWellAsAggregations() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | fields host", PATTERN));

    assertEquals(3, result.getJSONArray("datarows").length());
    JSONObject warning = result.getJSONArray("warnings").getJSONObject(0);
    assertEquals("PARTIAL_RESULT_SHARD_FAILURE", warning.getString("type"));
  }

  @Test
  public void everyShardAnsweringCarriesNoWarning() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | stats count() as n", HEALTHY_INDEX));

    verifyDataRows(result, rows(3));
    assertTrue("a complete result carries no warning", !result.has("warnings"));
  }

  @Test
  public void filteredQueryOverAHealthyIndexCarriesNoWarning() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | where bytes >= 900 | stats count() as n", HEALTHY_INDEX));

    verifyDataRows(result, rows(2));
    assertTrue("a complete result carries no warning", !result.has("warnings"));
  }
}
