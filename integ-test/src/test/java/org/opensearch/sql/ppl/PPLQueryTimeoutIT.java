/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.postPpl;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

/**
 * A PPL query that exceeds {@code plugins.ppl.query.timeout} fails with {@code 408}. Deadlines are
 * set above planning time so they expire during execution, which is where the status was lost.
 */
public class PPLQueryTimeoutIT extends PPLIntegTestCase {

  private static final String QUERY_TIMEOUT = "plugins.ppl.query.timeout";
  private static final String COMPLEX_POOL_ENABLED = "plugins.sql.complex_worker_pool.enabled";

  private static final String SLOW_INDEX = "ppl_query_timeout_slow_fixture";

  /** Pages through the index serially, so it outlasts the deadlines set below. */
  private static final String SLOW_QUERY =
      "source=" + SLOW_INDEX + " | streamstats count() as c | stats max(c) as total";

  private static final String FAST_QUERY = "source=" + SLOW_INDEX + " | head 1 | fields n";

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
    AsyncPPLTestHelpers.createIndex(client(), SLOW_INDEX, 100_000);
  }

  @After
  public void cleanUp() throws Exception {
    setTransient(QUERY_TIMEOUT, null);
    setTransient(COMPLEX_POOL_ENABLED, null);
  }

  /** The complex worker pool is on by default and takes any plan with scripts. */
  @Test
  public void queryExceedingTheDeadlineOnTheComplexPoolFailsWith408() throws Exception {
    setTransient(QUERY_TIMEOUT, "1s");

    assertTimedOut(SLOW_QUERY, "1s");
  }

  /** With the complex pool off the query runs synchronously on sql-worker. */
  @Test
  public void queryExceedingTheDeadlineOnTheWorkerPoolFailsWith408() throws Exception {
    setTransient(COMPLEX_POOL_ENABLED, false);
    setTransient(QUERY_TIMEOUT, "1s");

    assertTimedOut(SLOW_QUERY, "1s");
  }

  /** The deadline must not cut short a query that stays inside it. */
  @Test
  public void queryWithinTheDeadlineStillSucceeds() throws Exception {
    setTransient(QUERY_TIMEOUT, "60s");

    JSONObject response =
        new JSONObject(postPpl(client(), new JSONObject().put("query", FAST_QUERY)));
    Assert.assertEquals(1, response.getJSONArray("datarows").length());
  }

  /**
   * Asserts a 408 raised from execution. Planning raises the same type from {@code
   * InterruptibleRelRule}, so the messages are told apart; the complex-worker path prefixes its
   * reason, so the execution message is matched as a substring.
   */
  private void assertTimedOut(String query, String deadline) throws IOException {
    ResponseException e =
        Assert.assertThrows(
            ResponseException.class, () -> postPpl(client(), new JSONObject().put("query", query)));

    Assert.assertEquals(408, e.getResponse().getStatusLine().getStatusCode());

    JSONObject body = new JSONObject(getResponseBody(e.getResponse(), true));
    Assert.assertEquals(408, body.getInt("status"));
    Assert.assertEquals("QueryTimeoutException", body.getJSONObject("error").getString("type"));
    String reason = body.getJSONObject("error").getString("reason");
    Assert.assertTrue(reason, reason.contains("Query execution timed out after " + deadline));
    Assert.assertFalse(reason, reason.contains("Query planning interrupted"));
  }

  private void setTransient(String key, Object value) throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    request.setJsonEntity(
        new JSONObject()
            .put("transient", new JSONObject().put(key, value == null ? JSONObject.NULL : value))
            .toString());
    client().performRequest(request);
  }
}
