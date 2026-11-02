/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.INDEX;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.getAsyncQuery;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.postPpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/** Rows produced so far on async PPL responses (#5798). */
public class AsyncPPLPartialRowsIT extends PPLIntegTestCase {

  private static final long TIMEOUT_MILLIS = 60_000;

  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.TIMEWRAP_TEST);
    AsyncPPLTestHelpers.createIndex(client());
    enableCalcite();
  }

  @Test
  public void scanReturnsRowsBeforeCompletion() throws Exception {
    List<JSONObject> running = new ArrayList<>();
    JSONObject terminal = run("source=" + INDEX + " | fields n", running);

    assertMatchesSync("source=" + INDEX + " | fields n", terminal, running);
    Assert.assertTrue(
        "a 1,000-page scan must return rows before it finishes: " + sizes(running),
        running.stream().anyMatch(r -> rows(r).length() > 0));
  }

  @Test
  public void rowLocalOperatorsReturnRowsBeforeCompletion() throws Exception {
    String query = "source=" + INDEX + " | eval m = n * 2 | where m >= 0 | fields n, m";
    List<JSONObject> running = new ArrayList<>();
    JSONObject terminal = run(query, running);

    assertMatchesSync(query, terminal, running);
    Assert.assertTrue(running.stream().anyMatch(r -> rows(r).length() > 0));
  }

  @Test
  public void compositePagesBecomeVisible() throws Exception {
    // Small pages, so the aggregation spans many.
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting("persistent", "plugins.query.buckets", "100"));
    try {
      String query = "source=" + INDEX + " | stats count() as c by n";
      List<JSONObject> running = new ArrayList<>();
      JSONObject terminal = run(query, running);

      assertMatchesSync(query, terminal, running);
      Assert.assertTrue(
          "100 composite pages must return rows before the last one: " + sizes(running),
          running.stream().anyMatch(r -> rows(r).length() > 0));
    } finally {
      updateClusterSettings(
          new SQLIntegTestCase.ClusterSetting("persistent", "plugins.query.buckets", null));
    }
  }

  @Test
  public void blockingPlanReturnsRowsOnlyAfterReadingAllInput() throws Exception {
    String query = "source=" + INDEX + " | eventstats max(n) as mx | fields n, mx";
    List<JSONObject> running = new ArrayList<>();
    JSONObject terminal = run(query, running);

    assertMatchesSync(query, terminal, running);
    // eventstats emits only after reading all input.
    for (JSONObject response : running) {
      if (rows(response).length() > 0) {
        Assert.assertEquals(
            "rows before the scan finished: " + response.getJSONObject("progress"),
            0.95D,
            response.getJSONObject("progress").getDouble("fraction_done"),
            0D);
      }
    }
  }

  @Test
  public void rowsStopAtQuerySizeLimit() throws Exception {
    setQuerySizeLimit(500);
    try {
      String query = "source=" + INDEX + " | eval m = n | fields n, m";
      List<JSONObject> running = new ArrayList<>();
      JSONObject terminal = run(query, running);

      assertMatchesSync(query, terminal, running);
      Assert.assertEquals(500, terminal.getInt("total"));
      running.forEach(r -> Assert.assertTrue(rows(r).length() <= 500));
    } finally {
      resetQuerySizeLimit();
    }
  }

  @Test
  public void timewrapReturnsNoRowsUntilDone() throws Exception {
    String query =
        "source=timewrap_test | where @timestamp >= '2024-07-01 00:00:00' and @timestamp <="
            + " '2024-07-04 06:00:00' | timechart span=6h sum(requests) | timewrap 1day";
    List<JSONObject> running = new ArrayList<>();
    JSONObject terminal = run(query, running);

    assertMatchesSync(query, terminal, running);
    running.forEach(r -> Assert.assertEquals(0, rows(r).length()));
  }

  @Test
  public void failureAfterRowsReturnsNoRows() throws Exception {
    // Overflows on a late row, after earlier rows have been published.
    String query =
        "source=" + INDEX + " | eval x = if(n > 9000, n * 9223372036854775807, n) | fields x";
    List<JSONObject> running = new ArrayList<>();
    JSONObject terminal = run(query, running);

    Assert.assertEquals("FAILED", terminal.getString("status"));
    Assert.assertFalse("a failed job returns no rows", terminal.has("datarows"));
    Assert.assertTrue(
        "rows must have been visible before the failure: " + sizes(running),
        running.stream().anyMatch(r -> rows(r).length() > 0));
  }

  private JSONObject run(String query, List<JSONObject> running) throws Exception {
    JSONObject submit =
        new JSONObject(
            postPpl(
                client(),
                new JSONObject().put("query", query).put("wait_for_completion_timeout", "0")));
    Assert.assertEquals("RUNNING", submit.getString("status"));
    running.add(submit);
    String id = submit.getString("id");
    long deadline = System.currentTimeMillis() + TIMEOUT_MILLIS;
    while (System.currentTimeMillis() < deadline) {
      JSONObject response = new JSONObject(getAsyncQuery(client(), id));
      if (!"RUNNING".equals(response.getString("status"))) {
        return response;
      }
      running.add(response);
      Thread.sleep(20);
    }
    throw new AssertionError("async job did not finish: " + sizes(running));
  }

  /** The final result equals sync, and each running response is a prefix of it. */
  private void assertMatchesSync(String query, JSONObject terminal, List<JSONObject> running)
      throws IOException {
    Assert.assertEquals("SUCCEEDED", terminal.getString("status"));
    JSONObject sync = new JSONObject(postPpl(client(), new JSONObject().put("query", query)));
    Assert.assertTrue(
        "async schema must equal sync: sync="
            + sync.getJSONArray("schema")
            + " async="
            + terminal.getJSONArray("schema"),
        sync.getJSONArray("schema").similar(terminal.getJSONArray("schema")));
    Assert.assertTrue(
        "async rows must equal sync", sync.getJSONArray("datarows").similar(rows(terminal)));

    JSONArray finalRows = rows(terminal);
    int previous = 0;
    for (JSONObject response : running) {
      JSONArray partial = rows(response);
      Assert.assertEquals(partial.length(), response.getInt("total"));
      Assert.assertTrue("rows must only grow: " + sizes(running), partial.length() >= previous);
      previous = partial.length();
      for (int i = 0; i < partial.length(); i++) {
        Assert.assertTrue(
            "running row " + i + " must equal the final row",
            finalRows.getJSONArray(i).similar(partial.getJSONArray(i)));
      }
      if (partial.length() > 0) {
        Assert.assertTrue(
            "running schema must equal the final schema",
            terminal.getJSONArray("schema").similar(response.getJSONArray("schema")));
      }
    }
  }

  private static JSONArray rows(JSONObject response) {
    return response.getJSONArray("datarows");
  }

  private static List<Integer> sizes(List<JSONObject> responses) {
    return responses.stream().map(r -> rows(r).length()).toList();
  }
}
