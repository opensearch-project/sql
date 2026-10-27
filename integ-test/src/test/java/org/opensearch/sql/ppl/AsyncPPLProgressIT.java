/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.INDEX;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.OVERFLOW_QUERY;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.STREAMSTATS_QUERY;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.getAsyncQuery;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.postPpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies {@code progress.fraction_done} on async PPL responses (issue #5797): bounded and
 * monotonic while running, {@code 1.0} on success, the last running value on failure, and absent
 * from synchronous responses.
 */
public class AsyncPPLProgressIT extends PPLIntegTestCase {

  private static final long TIMEOUT_MILLIS = 60_000;

  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    AsyncPPLTestHelpers.createIndex(client());
    enableCalcite();
  }

  @Test
  public void syncResponsesCarryNoProgress() throws IOException {
    String query = "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c";
    JSONObject sync = new JSONObject(postPpl(client(), new JSONObject().put("query", query)));
    Assert.assertFalse(sync.has("progress"));

    JSONObject inline =
        new JSONObject(
            postPpl(
                client(),
                new JSONObject().put("query", query).put("wait_for_completion_timeout", "30s")));
    Assert.assertFalse("fast success keeps the sync shape", inline.has("progress"));
  }

  @Test
  public void pagedScanProgressIsBoundedMonotonicAndOneOnSuccess() throws Exception {
    JSONObject submit = submit(STREAMSTATS_QUERY);
    List<Double> running = new ArrayList<>(List.of(fraction(submit)));
    JSONObject terminal = pollRecordingProgress(submit.getString("id"), running);

    Assert.assertEquals("SUCCEEDED", terminal.getString("status"));
    Assert.assertEquals(1D, fraction(terminal), 0D);
    assertBoundedAndMonotonic(running);
    Assert.assertTrue(
        "a 1,000-page scan must report progress before it finishes: " + running,
        running.stream().anyMatch(value -> value > 0D));
  }

  @Test
  public void rootScanProgressIsBoundedMonotonicAndOneOnSuccess() throws Exception {
    // A fully pushed scan is the plan root and runs without generated code.
    JSONObject submit = submit("source=" + INDEX + " | fields n");
    List<Double> running = new ArrayList<>(List.of(fraction(submit)));
    JSONObject terminal = pollRecordingProgress(submit.getString("id"), running);

    Assert.assertEquals("SUCCEEDED", terminal.getString("status"));
    Assert.assertEquals(1D, fraction(terminal), 0D);
    assertBoundedAndMonotonic(running);
    Assert.assertTrue(
        "a 1,000-page root scan must report progress before it finishes: " + running,
        running.stream().anyMatch(value -> value > 0D));
  }

  @Test
  public void multiSourceProgressIsBoundedMonotonicAndOneOnSuccess() throws Exception {
    String query =
        STREAMSTATS_QUERY + " | append [ source=" + TEST_INDEX_ACCOUNT + " | stats count() as c ]";
    JSONObject submit = submit(query);
    List<Double> running = new ArrayList<>(List.of(fraction(submit)));
    JSONObject terminal = pollRecordingProgress(submit.getString("id"), running);

    Assert.assertEquals("SUCCEEDED", terminal.getString("status"));
    Assert.assertEquals(1D, fraction(terminal), 0D);
    assertBoundedAndMonotonic(running);
  }

  @Test
  public void compositeAggregationProgressIsOneOnSuccess() throws Exception {
    JSONObject submit = submit("source=" + INDEX + " | stats count() by n");
    List<Double> running = new ArrayList<>(List.of(fraction(submit)));
    JSONObject terminal = pollRecordingProgress(submit.getString("id"), running);

    Assert.assertEquals("SUCCEEDED", terminal.getString("status"));
    Assert.assertEquals(1D, fraction(terminal), 0D);
    assertBoundedAndMonotonic(running);
  }

  @Test
  public void failureRetainsLastRunningProgress() throws Exception {
    JSONObject submit = submit(OVERFLOW_QUERY);
    List<Double> running = new ArrayList<>(List.of(fraction(submit)));
    JSONObject terminal = pollRecordingProgress(submit.getString("id"), running);

    Assert.assertEquals("FAILED", terminal.getString("status"));
    double failed = fraction(terminal);
    Assert.assertTrue("failure must not report completion: " + failed, failed < 1D);
    Assert.assertTrue(
        "failure must keep the last running value: " + running + " then " + failed,
        failed >= running.get(running.size() - 1));
  }

  private JSONObject submit(String query) throws IOException {
    String body =
        postPpl(
            client(), new JSONObject().put("query", query).put("wait_for_completion_timeout", "0"));
    // Rendered as a double, like the GET body, even when it is a whole number.
    Assert.assertTrue(body, body.matches("(?s).*\"fraction_done\"\\s*:\\s*\\d+\\.\\d+.*"));
    JSONObject response = new JSONObject(body);
    Assert.assertEquals("RUNNING", response.getString("status"));
    return response;
  }

  /** Polls until terminal, appending each running {@code fraction_done} to {@code running}. */
  private JSONObject pollRecordingProgress(String id, List<Double> running) throws Exception {
    long deadline = System.currentTimeMillis() + TIMEOUT_MILLIS;
    while (System.currentTimeMillis() < deadline) {
      JSONObject response = new JSONObject(getAsyncQuery(client(), id));
      if (!"RUNNING".equals(response.getString("status"))) {
        return response;
      }
      running.add(fraction(response));
      Thread.sleep(20);
    }
    throw new AssertionError("async job on " + INDEX + " did not finish: " + running);
  }

  private static double fraction(JSONObject response) {
    return response.getJSONObject("progress").getDouble("fraction_done");
  }

  private static void assertBoundedAndMonotonic(List<Double> running) {
    for (int i = 0; i < running.size(); i++) {
      double value = running.get(i);
      Assert.assertTrue("running progress out of [0, 1): " + running, value >= 0D && value < 1D);
      if (i > 0) {
        Assert.assertTrue("progress decreased: " + running, value >= running.get(i - 1));
      }
    }
  }
}
