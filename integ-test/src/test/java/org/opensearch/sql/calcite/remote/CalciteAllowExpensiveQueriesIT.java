/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * A predicate the index cannot answer is planned as a {@code script} query, which OpenSearch
 * classifies as expensive and the shard refuses when {@code search.allow_expensive_queries} is
 * false. Issue #5837: the plugin consults the setting at planning time and leaves such a predicate
 * above the scan instead of sending a query the shard will reject.
 */
public class CalciteAllowExpensiveQueriesIT extends PPLIntegTestCase {

  private static final String INDEX = "expensive_queries_5837";
  private static final String SETTING = "search.allow_expensive_queries";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    createIndex();
  }

  @After
  public void resetSetting() throws IOException {
    updateClusterSettings(new ClusterSetting(TRANSIENT, SETTING, null));
  }

  /** {@code msg} has no keyword sub-field, so equality on it cannot become a term query. */
  private void createIndex() throws IOException {
    try {
      client().performRequest(new Request("DELETE", "/" + INDEX));
    } catch (ResponseException ignored) {
      // the index is not there yet
    }
    Request createIndex = new Request("PUT", "/" + INDEX);
    createIndex.setJsonEntity(
        "{\"mappings\":{\"properties\":{"
            + "\"id\":{\"type\":\"keyword\"},"
            + "\"msg\":{\"type\":\"text\"}}}}");
    client().performRequest(createIndex);

    Request bulk = new Request("POST", "/" + INDEX + "/_bulk?refresh=true");
    bulk.setJsonEntity(
        "{\"index\":{}}\n"
            + "{\"id\":\"a\",\"msg\":\"hello world\"}\n"
            + "{\"index\":{}}\n"
            + "{\"id\":\"b\",\"msg\":\"goodbye world\"}\n");
    client().performRequest(bulk);
  }

  @Test
  public void testScriptPredicateAnsweredWhenExpensiveQueriesAreOff() throws IOException {
    updateClusterSettings(new ClusterSetting(TRANSIENT, SETTING, "false"));

    JSONObject result =
        executeQuery(String.format("source=%s | where msg = 'hello world' | fields id", INDEX));
    verifyDataRows(result, rows("a"));
  }

  @Test
  public void testScriptPredicateIsNotPushedDownWhenExpensiveQueriesAreOff() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String query = String.format("source=%s | where msg = 'hello world' | fields id", INDEX);

    updateClusterSettings(new ClusterSetting(TRANSIENT, SETTING, "true"));
    assertTrue(
        "a predicate the index cannot answer is a script pushdown while the setting is on",
        explainQueryToString(query).contains("script"));

    updateClusterSettings(new ClusterSetting(TRANSIENT, SETTING, "false"));
    assertFalse(
        "no script reaches the index while the setting is off",
        explainQueryToString(query).contains("script"));
  }

  /**
   * Only the part of the predicate that needs a script is left above the scan: what the index can
   * answer is still pushed down, so the setting does not turn the query into a full scan.
   */
  @Test
  public void testTheRestOfThePredicateIsStillPushedDown() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    updateClusterSettings(new ClusterSetting(TRANSIENT, SETTING, "false"));
    String query =
        String.format("source=%s | where id = 'a' and msg = 'hello world' | fields id", INDEX);

    String explained = explainQueryToString(query);
    assertTrue("the term half still reaches the index", explained.contains("\\\"id\\\":"));
    assertFalse("the script half does not", explained.contains("script"));

    verifyDataRows(executeQuery(query), rows("a"));
  }

  /**
   * {@code stats count(eval(...))} plans the predicate as a filter aggregation, which carries the
   * same script query. The whole aggregation is left for the coordinator rather than refused.
   */
  @Test
  public void testFilteredAggregationAnsweredWhenExpensiveQueriesAreOff() throws IOException {
    updateClusterSettings(new ClusterSetting(TRANSIENT, SETTING, "false"));

    JSONObject result =
        executeQuery(
            String.format("source=%s | stats count(eval(msg = 'hello world')) as c", INDEX));
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testIndexAnswerablePredicateIsUnaffected() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    updateClusterSettings(new ClusterSetting(TRANSIENT, SETTING, "false"));
    String query = String.format("source=%s | where id = 'a' | fields id", INDEX);

    assertTrue(
        "a term query is not expensive and is still pushed down",
        explainQueryToString(query).contains("\\\"id\\\":"));
    verifyDataRows(executeQuery(query), rows("a"));
  }
}
