/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for the {@code rest} leading command on the Calcite path. Uses {@code
 * /_cluster/health} as the deterministic, single-row endpoint on a single-node test cluster. Also verifies that a non-allow-listed / mutating endpoint is refused.
 */
public class CalcitePPLRestIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Test
  public void testRestClusterHealthSchema() throws IOException {
    JSONObject result =
        executeQuery("| rest '/_cluster/health' | fields status, number_of_nodes");
    verifySchema(result, schema("status", "string"), schema("number_of_nodes", "int"));
  }

  @Test
  public void testRestClusterHealthDataRows() throws IOException {
    // Single-node test cluster: exactly one node, status is green or yellow.
    JSONObject result = executeQuery("| rest '/_cluster/health' | fields number_of_nodes");
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testRestRejectsNonAllowListedEndpoint() throws IOException {
    assertRestBadRequest("| rest '/_cluster/reroute'", "allow-list");
  }

  @Test
  public void testRestRejectsEmptyEndpoint() throws IOException {
    assertRestBadRequest("| rest ''", "non-empty path");
  }

  @Test
  public void testRestRejectsDisallowedArg() throws IOException {
    assertRestBadRequest("| rest '/_cat/nodes' h='name'", "does not accept arg");
  }

  @Test
  public void testRestRejectsNegativeCount() throws IOException {
    assertRestBadRequest("| rest '/_cat/nodes' count=-1", "non-negative");
  }

  @Test
  public void testRestRejectsTimeoutArg() throws IOException {
    assertRestBadRequest("| rest '/_cluster/health' timeout='5s'", "timeout");
  }

  /**
   * Assert a {@code rest} query is refused as a client error: HTTP 400 (not a 500 system error)
   * with the given substring in the response body. The negative-case check for allow-list and secret-filter enforcement.
   */
  private void assertRestBadRequest(String query, String expectedSubstring) {
    ResponseException e =
        org.junit.Assert.assertThrows(ResponseException.class, () -> executeQuery(query));
    org.junit.Assert.assertEquals(
        400, e.getResponse().getStatusLine().getStatusCode());
    org.junit.Assert.assertTrue(
        "expected [" + expectedSubstring + "] in response body: " + e.getMessage(),
        e.getMessage().contains(expectedSubstring));
  }

  @Test
  public void testRestCatIndicesSchema() throws IOException {
    // Schema is fixed by the registry, independent of how many indices exist.
    JSONObject result = executeQuery("| rest '/_cat/indices' | fields index, health");
    verifySchema(result, schema("index", "string"), schema("health", "string"));
  }

  @Test
  public void testRestCatIndicesReturnsCreatedIndex() throws IOException {
    // Create a known index, then confirm rest surfaces it and downstream where/fields compose.
    client().performRequest(new Request("PUT", "/rest_cat_test"));
    JSONObject result =
        executeQuery("| rest '/_cat/indices' | where index = 'rest_cat_test' | fields index");
    verifyDataRows(result, rows("rest_cat_test"));
  }

  @Test
  public void testRestCatNodesSchema() throws IOException {
    JSONObject result = executeQuery("| rest '/_cat/nodes' | fields name, cpu");
    verifySchema(result, schema("name", "string"), schema("cpu", "int"));
  }

  @Test
  public void testRestCatNodesSingleNode() throws IOException {
    JSONObject result = executeQuery("| rest '/_cat/nodes' | stats count() as cnt");
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testRestCatClusterManagerSchema() throws IOException {
    JSONObject result = executeQuery("| rest '/_cat/cluster_manager' | fields node, id");
    verifySchema(result, schema("node", "string"), schema("id", "string"));
  }

  @Test
  public void testRestCatClusterManagerSingleRow() throws IOException {
    JSONObject result = executeQuery("| rest '/_cat/cluster_manager' | stats count() as cnt");
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testRestCatPluginsSchema() throws IOException {
    JSONObject result = executeQuery("| rest '/_cat/plugins' | fields component, version");
    verifySchema(result, schema("component", "string"), schema("version", "string"));
  }

  @Test
  public void testRestCatShardsSchema() throws IOException {
    JSONObject result = executeQuery("| rest '/_cat/shards' | fields index, shard, state");
    verifySchema(
        result, schema("index", "string"), schema("shard", "int"), schema("state", "string"));
  }

  @Test
  public void testRestClusterStateSchema() throws IOException {
    // Assert the string columns; version is the LONG epoch column.
    JSONObject result =
        executeQuery("| rest '/_cluster/state' | fields cluster_name, cluster_manager_node");
    verifySchema(
        result, schema("cluster_name", "string"), schema("cluster_manager_node", "string"));
  }

  @Test
  public void testRestClusterStateSingleRow() throws IOException {
    JSONObject result = executeQuery("| rest '/_cluster/state' | stats count() as cnt");
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testRestClusterSettingsSchema() throws IOException {
    // Schema is registry-fixed regardless of how many settings are configured.
    JSONObject result =
        executeQuery("| rest '/_cluster/settings' | fields setting, value, tier");
    verifySchema(
        result, schema("setting", "string"), schema("value", "string"), schema("tier", "string"));
  }

  @Test
  public void testRestResolveIndexSchema() throws IOException {
    JSONObject result = executeQuery("| rest '/_resolve/index' | fields name, type");
    verifySchema(result, schema("name", "string"), schema("type", "string"));
  }

  @Test
  public void testRestResolveIndexSurfacesCreatedIndex() throws IOException {
    client().performRequest(new Request("PUT", "/rest_resolve_test"));
    JSONObject result =
        executeQuery(
            "| rest '/_resolve/index' | where name = 'rest_resolve_test' | fields name, type");
    verifyDataRows(result, rows("rest_resolve_test", "index"));
  }

  // ---- get-arg server-side filtering (health, expand_wildcards, local) ----

  @Test
  public void testRestClusterHealthLocalArg() throws IOException {
    // local=true reads health from the local node; on a single-node cluster the row is unchanged.
    JSONObject result =
        executeQuery("| rest '/_cluster/health' local='true' | fields number_of_nodes");
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testRestCatIndicesHealthFilterReturnsNoRed() throws IOException {
    // health filters rows server-side; a healthy cluster has no red indices, so count is 0.
    JSONObject result =
        executeQuery("| rest '/_cat/indices' health='red' | stats count() as cnt");
    verifyDataRows(result, rows(0));
  }

  @Test
  public void testRestResolveIndexExpandWildcardsArg() throws IOException {
    // expand_wildcards is applied to the resolve request; schema stays fixed and the call succeeds.
    JSONObject result =
        executeQuery("| rest '/_resolve/index' expand_wildcards='open' | fields name, type");
    verifySchema(result, schema("name", "string"), schema("type", "string"));
  }

  @Test
  public void testRestRejectsDroppedLevelArg() throws IOException {
    // level was dropped from the allow-list (no-op against the fixed health schema).
    assertRestBadRequest("| rest '/_cluster/health' level='indices'", "does not accept arg");
  }

  @Test
  public void testRestRejectsBadArgValue() throws IOException {
    assertRestBadRequest("| rest '/_cat/indices' health='purple'", "unsupported value");
  }
}
