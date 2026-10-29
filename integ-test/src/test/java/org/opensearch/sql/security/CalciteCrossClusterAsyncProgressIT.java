/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.AsyncPPLTestHelpers;

/** Async PPL progress (#5797) on remote and mixed local/remote indices. */
public class CalciteCrossClusterAsyncProgressIT extends CrossClusterTestBase {

  private static final String REMOTE_FIXTURE = REMOTE_CLUSTER + ":" + AsyncPPLTestHelpers.INDEX;
  private static final long TIMEOUT_MILLIS = 60_000;

  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.ACCOUNT, remoteClient());
    AsyncPPLTestHelpers.createIndex(remoteClient());
    enableCalcite();
  }

  @Test
  public void remoteScanReportsProgress() throws Exception {
    List<Double> running = new ArrayList<>();
    JSONObject terminal = runAsync("source=" + REMOTE_FIXTURE + " | fields n", running);

    Assert.assertEquals("SUCCEEDED", terminal.getString("status"));
    Assert.assertEquals(1D, fraction(terminal), 0D);
    Assert.assertEquals(10_000, terminal.getInt("total"));
    assertBoundedAndMonotonic(running);
    Assert.assertTrue(
        "a 1,000-page remote scan must report progress before it finishes: " + running,
        running.stream().anyMatch(value -> value > 0D));
  }

  @Test
  public void remoteBlockingScanReportsProgress() throws Exception {
    // The size limit is not pushed below streamstats, so the remote PIT scan has no row limit.
    String query =
        "source="
            + REMOTE_FIXTURE
            + " | streamstats count() as running_count | stats max(running_count) as total";
    List<Double> running = new ArrayList<>();
    JSONObject terminal = runAsync(query, running);

    Assert.assertEquals("SUCCEEDED", terminal.getString("status"));
    Assert.assertEquals(1D, fraction(terminal), 0D);
    assertSameRows(query, terminal);
    assertBoundedAndMonotonic(running);
    Assert.assertTrue(running.stream().anyMatch(value -> value > 0D));
  }

  @Test
  public void remoteAggregationMatchesSync() throws Exception {
    String query = "source=" + TEST_INDEX_ACCOUNT_REMOTE + " | stats count() by gender";
    List<Double> running = new ArrayList<>();
    JSONObject terminal = runAsync(query, running);

    Assert.assertEquals("SUCCEEDED", terminal.getString("status"));
    Assert.assertEquals(1D, fraction(terminal), 0D);
    assertSameRows(query, terminal);
    assertBoundedAndMonotonic(running);
  }

  @Test
  public void localAndRemoteSourceMatchesSync() throws Exception {
    String query =
        "source=" + TEST_INDEX_ACCOUNT + "," + TEST_INDEX_ACCOUNT_REMOTE + " | stats count() as c";
    List<Double> running = new ArrayList<>();
    JSONObject terminal = runAsync(query, running);

    Assert.assertEquals("SUCCEEDED", terminal.getString("status"));
    Assert.assertEquals(1D, fraction(terminal), 0D);
    assertSameRows(query, terminal);
    assertBoundedAndMonotonic(running);
  }

  /** Submits with a zero wait, then polls until terminal, collecting every running sample. */
  private JSONObject runAsync(String query, List<Double> running) throws Exception {
    JSONObject submit =
        post(new JSONObject().put("query", query).put("wait_for_completion_timeout", "0"));
    Assert.assertEquals("RUNNING", submit.getString("status"));
    running.add(fraction(submit));
    String id = submit.getString("id");
    long deadline = System.currentTimeMillis() + TIMEOUT_MILLIS;
    while (System.currentTimeMillis() < deadline) {
      JSONObject response =
          new JSONObject(
              getResponseBody(
                  client().performRequest(new Request("GET", "/_plugins/_async_query/" + id)),
                  true));
      if (!"RUNNING".equals(response.getString("status"))) {
        return response;
      }
      running.add(fraction(response));
      Thread.sleep(20);
    }
    throw new AssertionError("async job did not finish: " + running);
  }

  private JSONObject post(JSONObject body) throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(body.toString());
    return new JSONObject(getResponseBody(client().performRequest(request), true));
  }

  private void assertSameRows(String query, JSONObject asyncTerminal) throws IOException {
    JSONObject sync = post(new JSONObject().put("query", query));
    Assert.assertTrue(
        "async rows must equal sync rows: sync=" + sync + " async=" + asyncTerminal,
        sync.getJSONArray("datarows").similar(asyncTerminal.getJSONArray("datarows")));
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
