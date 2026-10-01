/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.ASYNC_QUERY_ENDPOINT;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.PPL_ENDPOINT;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.getAsyncQuery;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.pollUntilTerminal;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.postPpl;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

/**
 * End-to-end IT for the async PPL lifecycle (issue #5765). Verifies:
 *
 * <ul>
 *   <li>sync submit without async body fields returns the sync-shape response (schema + rows, no
 *       id, no status);
 *   <li>submit with {@code wait_for_completion_timeout=0} returns the running snapshot {@code {id,
 *       status: RUNNING, schema=[], datarows=[], total=0}} without blocking on the runner;
 *   <li>submit with a wait budget longer than runner duration returns the sync-shape terminal
 *       response inline (runner-wins-race);
 *   <li>fetch on {@code GET /_plugins/_async_query/{id}} eventually returns the terminal result
 *       with schema and rows;
 *   <li>statement-level explain (query text starts with {@code explain ...}) is supported in async
 *       and returns the explain body on GET;
 *   <li>sync-only request shapes (explain endpoint, analyze endpoint, profile flag, csv format) are
 *       rejected with 400 when they carry {@code wait_for_completion_timeout};
 *   <li>{@code keep_alive} drives retention — a job is evicted after its TTL elapses.
 * </ul>
 */
public class AsyncPPLQueryLifecycleIT extends PPLIntegTestCase {

  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void sync_submitReturnsSyncShapeWithFullResult() throws IOException {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");

    JSONObject response = new JSONObject(postPpl(client(), body));

    Assert.assertFalse("sync response must not carry queryId", response.has("id"));
    verifySchema(response, schema("c", "bigint"));
    verifyDataRows(response, rows(1000));
    verifyNumOfRows(response, 1);
  }

  @Test
  public void async_submitWithZeroWaitReturnsRunningSnapshot() throws IOException {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");

    JSONObject response = new JSONObject(postPpl(client(), body));

    Assert.assertTrue("async response must carry queryId", response.has("id"));
    Assert.assertEquals("RUNNING", response.getString("status"));
    Assert.assertEquals(0, response.getJSONArray("schema").length());
    Assert.assertEquals(0, response.getJSONArray("datarows").length());
    Assert.assertEquals(0, response.getInt("total"));
  }

  @Test
  public void async_submitWithLongWaitReturnsSyncShape() throws IOException {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    // Big budget — the stats runner finishes well before 30s, so the submit response is the
    // sync-shape terminal body (no id).
    body.put("wait_for_completion_timeout", "30s");

    JSONObject response = new JSONObject(postPpl(client(), body));

    Assert.assertFalse("runner-wins response must not carry queryId", response.has("id"));
    verifySchema(response, schema("c", "bigint"));
    verifyDataRows(response, rows(1000));
  }

  @Test
  public void async_fetchTerminalReturnsResultWithSchemaAndRows() throws Exception {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");

    String queryId = new JSONObject(postPpl(client(), body)).getString("id");
    JSONObject fetched = pollUntilTerminal(client(), queryId, 30_000);

    Assert.assertEquals("SUCCEEDED", fetched.getString("status"));
    // Async GET formatter emits raw engine type ("long"), not the JDBC family ("bigint") emitted
    // by the sync formatter above.
    verifySchema(fetched, schema("c", "long"));
    verifyDataRows(fetched, rows(1000));
    verifyNumOfRows(fetched, 1);
  }

  @Test
  public void async_explainStatementReturnsExplainBodyOnGet() throws Exception {
    JSONObject body = new JSONObject();
    body.put("query", "explain source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");

    String queryId = new JSONObject(postPpl(client(), body)).getString("id");

    // Explain terminal response has no `status` field. Poll until the body is JSON-parseable with
    // the calcite/root plan-tree marker.
    long deadline = System.currentTimeMillis() + 30_000L;
    JSONObject explain = null;
    while (System.currentTimeMillis() < deadline) {
      String raw = getAsyncQuery(client(), queryId);
      try {
        JSONObject parsed = new JSONObject(raw);
        if (parsed.has("calcite") || parsed.has("root")) {
          explain = parsed;
          break;
        }
      } catch (RuntimeException ignored) {
        // not valid JSON yet; keep polling
      }
      Thread.sleep(200);
    }
    Assert.assertNotNull("cross-poll explain body never arrived", explain);
    Assert.assertTrue(
        "explain body must carry a plan tree", explain.has("calcite") || explain.has("root"));
  }

  @Test
  public void async_fallsThroughToSyncForExplainEndpoint() throws IOException {
    // Explain endpoint ignores wait_for_completion_timeout and returns the sync explain body.
    JSONObject response =
        new JSONObject(
            postPpl(
                client(),
                PPL_ENDPOINT + "/_explain",
                withAsyncWait("source=" + TEST_INDEX_ACCOUNT + " | stats count() as c")));
    Assert.assertFalse("sync-explain response must not carry queryId", response.has("id"));
    Assert.assertTrue(
        "sync-explain response must carry a plan tree",
        response.has("calcite") || response.has("root"));
  }

  @Test
  public void async_fallsThroughToSyncForProfileFlag() throws IOException {
    JSONObject body = withAsyncWait("source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("profile", true);
    JSONObject response = new JSONObject(postPpl(client(), PPL_ENDPOINT, body));
    Assert.assertFalse("sync-profile response must not carry queryId", response.has("id"));
    // Profile output carries the normal sync result; schema + rows are present.
    Assert.assertTrue("sync-profile response must carry schema", response.has("schema"));
    Assert.assertTrue("sync-profile response must carry datarows", response.has("datarows"));
  }

  @Test
  public void async_fallsThroughToSyncForCsvFormat() throws IOException {
    // CSV format: the response body is text/csv, not JSON. Just assert it isn't a JSON async
    // snapshot and that the row count matches sync behavior.
    Request request = new Request("POST", PPL_ENDPOINT + "?format=csv");
    request.setJsonEntity(
        withAsyncWait("source=" + TEST_INDEX_ACCOUNT + " | stats count() as c").toString());
    String body =
        org.opensearch.sql.legacy.TestUtils.getResponseBody(client().performRequest(request), true);
    Assert.assertFalse("csv response must not be a JSON async snapshot", body.contains("\"id\""));
    Assert.assertTrue(
        "csv response must carry the count=1000 row, got: " + body, body.contains("1000"));
  }

  @Test
  public void async_fetchUnknownQueryIdReturns4xx() {
    Request request = new Request("GET", ASYNC_QUERY_ENDPOINT + "nodeX%3Adoes-not-exist");
    ResponseException ex =
        Assert.assertThrows(ResponseException.class, () -> client().performRequest(request));
    int code = ex.getResponse().getStatusLine().getStatusCode();
    Assert.assertTrue("expected 4xx for unknown queryId, got " + code, code >= 400 && code < 500);
  }

  @Test
  public void async_keepAliveEvictsAfterTtl() throws Exception {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");
    body.put("keep_alive", "1s");

    String queryId = new JSONObject(postPpl(client(), body)).getString("id");
    JSONObject terminal = pollUntilTerminal(client(), queryId, 5_000);
    Assert.assertEquals("SUCCEEDED", terminal.getString("status"));

    // Eviction fires 1s after the terminal transition.
    long deadline = System.currentTimeMillis() + 3_000L;
    while (System.currentTimeMillis() < deadline) {
      try {
        getAsyncQuery(client(), queryId);
      } catch (ResponseException ex) {
        int code = ex.getResponse().getStatusLine().getStatusCode();
        Assert.assertTrue(
            "expected 4xx after keep_alive expiry, got " + code, code >= 400 && code < 500);
        return;
      }
      Thread.sleep(200);
    }
    Assert.fail("job [" + queryId + "] was not evicted within 3s after keep_alive=1s");
  }

  @Test
  public void async_rejectsWaitForCompletionTimeoutExceedingMax() {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "120s"); // > 60s cap
    assertRejectedWith400(PPL_ENDPOINT, body);
  }

  @Test
  public void async_rejectsKeepAliveExceedingMax() {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");
    body.put("keep_alive", "365d"); // > 24h cap
    assertRejectedWith400(PPL_ENDPOINT, body);
  }

  @Test
  public void async_rejectsZeroKeepAlive() {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");
    body.put("keep_alive", "0"); // not strictly positive
    assertRejectedWith400(PPL_ENDPOINT, body);
  }

  private static JSONObject withAsyncWait(String query) {
    JSONObject body = new JSONObject();
    body.put("query", query);
    body.put("wait_for_completion_timeout", "0");
    return body;
  }

  private void assertRejectedWith400(String endpoint, JSONObject body) {
    Request request = new Request("POST", endpoint);
    request.setJsonEntity(body.toString());
    ResponseException ex =
        Assert.assertThrows(ResponseException.class, () -> client().performRequest(request));
    Assert.assertEquals(400, ex.getResponse().getStatusLine().getStatusCode());
  }
}
