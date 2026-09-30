/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

/**
 * End-to-end IT for the async PPL lifecycle (issue #5765). Verifies:
 *
 * <ul>
 *   <li>sync submit without async body fields keeps the current behavior (no id, no status);
 *   <li>submit with {@code wait_for_completion_timeout=0} returns {@code {id, status: RUNNING,
 *       ...}} without blocking on the runner;
 *   <li>fetch on {@code GET /_plugins/_async_query/{id}} eventually returns the terminal result.
 * </ul>
 */
public class AsyncPPLQueryLifecycleIT extends PPLIntegTestCase {

  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void sync_submitReturnsSyncResponse() throws IOException {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    JSONObject response = new JSONObject(post(body));
    // Sync response shape: schema + datarows, no id, no status.
    Assert.assertTrue("sync response must expose schema", response.has("schema"));
    Assert.assertFalse("sync response must not carry queryId", response.has("id"));
  }

  @Test
  public void async_submitReturnsRunningWithQueryId() throws IOException {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");
    JSONObject response = new JSONObject(post(body));
    Assert.assertTrue("async response must carry queryId", response.has("id"));
    Assert.assertEquals("RUNNING", response.getString("status"));
  }

  @Test
  public void async_fetchEventuallyReturnsTerminal() throws Exception {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");
    String queryId = new JSONObject(post(body)).getString("id");

    JSONObject fetched = pollUntilTerminal(queryId);
    Assert.assertTrue(
        "fetch response must be terminal",
        "SUCCEEDED".equals(fetched.getString("status"))
            || "FAILED".equals(fetched.getString("status")));
  }

  @Test
  public void async_explainStatementReturnsExplainBody() throws Exception {
    JSONObject body = new JSONObject();
    body.put("query", "explain source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");
    String queryId = new JSONObject(post(body)).getString("id");

    // GET returns the explain body verbatim — it has no `status` field, so poll on the raw text.
    long deadline = System.currentTimeMillis() + 30_000L;
    String raw = null;
    while (System.currentTimeMillis() < deadline) {
      raw = get(queryId);
      if (raw.contains("\"calcite\"") || raw.contains("\"root\"")) {
        return;
      }
      Thread.sleep(200);
    }
    Assert.fail("explain body never appeared; last=" + raw);
  }

  private JSONObject pollUntilTerminal(String queryId) throws Exception {
    long deadline = System.currentTimeMillis() + 30_000L;
    JSONObject last = null;
    while (System.currentTimeMillis() < deadline) {
      last = new JSONObject(get(queryId));
      String status = last.getString("status");
      if (!"RUNNING".equals(status) && !"PENDING".equals(status)) {
        return last;
      }
      Thread.sleep(200);
    }
    Assert.fail("async job did not reach terminal status within 30s. last=" + last);
    return last; // unreachable
  }

  private String post(JSONObject body) throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(body.toString());
    Response response = client().performRequest(request);
    return getResponseBody(response, true);
  }

  private String get(String queryId) throws IOException {
    Request request = new Request("GET", "/_plugins/_async_query/" + queryId);
    Response response = client().performRequest(request);
    return getResponseBody(response, true);
  }
}
