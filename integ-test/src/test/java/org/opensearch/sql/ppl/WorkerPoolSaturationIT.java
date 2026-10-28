/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.STREAMSTATS_QUERY;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.awaitPoolBusy;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.awaitPoolsIdle;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.deleteAsyncQuery;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.localNodeId;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.pollUntilTerminal;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.postPpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Cancellable;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.ResponseListener;
import org.opensearch.sql.plugin.transport.PPLQueryAction;

/**
 * PPL queries refused by a saturated SQL worker pool fail with {@code 429} (#5801).
 *
 * <p>Runs on the dedicated single-node {@code workerPoolSaturationIT} cluster, whose {@code
 * sql-worker} and {@code sql-complex-worker} pools each have one thread and a one-slot queue, so
 * saturation is deterministic.
 */
public class WorkerPoolSaturationIT extends PPLIntegTestCase {

  private static final String COMPLEX_POOL = "sql-complex-worker";
  private static final String SATURATED =
      "[sql-complex-worker] thread pool is saturated; retry later";

  /** Ten thousand serial searches: outlasts every saturation check, stops promptly on cancel. */
  private static final String SLOW_INDEX = "worker_pool_saturation_slow_fixture";

  private static final String SLOW_QUERY =
      "source=" + SLOW_INDEX + " | streamstats count() as c | stats max(c) as total";

  private final List<Cancellable> busy = new ArrayList<>();
  private final List<String> jobIds = new ArrayList<>();

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
    AsyncPPLTestHelpers.createIndex(client());
    AsyncPPLTestHelpers.createIndex(client(), SLOW_INDEX, 100_000);
  }

  @After
  public void cleanUp() throws Exception {
    // Cancel the sync queries' tasks, then drop the requests.
    client().performRequest(new Request("POST", "/_tasks/_cancel?actions=" + PPLQueryAction.NAME));
    busy.forEach(Cancellable::cancel);
    busy.clear();
    for (String id : jobIds) {
      try {
        deleteAsyncQuery(client(), id);
      } catch (ResponseException e) {
        // Already deleted, evicted, or failed inline.
      }
    }
    jobIds.clear();
    awaitPoolsIdle(client(), localNodeId(client()));
  }

  @Test
  public void syncQueryRefusedBySaturatedPoolFailsWith429() throws Exception {
    saturateComplexPool();

    ResponseException e =
        Assert.assertThrows(
            ResponseException.class,
            () -> postPpl(client(), new JSONObject().put("query", SLOW_QUERY)));

    Assert.assertEquals(429, e.getResponse().getStatusLine().getStatusCode());
    Assert.assertTrue(errorBody(e).contains(SATURATED));
  }

  @Test
  public void asyncQueryRefusedBySaturatedPoolFailsWith429() throws Exception {
    saturateComplexPool();

    // The pool refuses it after planning, so it fails inline with 429 or, once its id was
    // returned, ends FAILED.
    JSONObject async;
    try {
      async =
          new JSONObject(
              postPpl(
                  client(),
                  new JSONObject()
                      .put("query", STREAMSTATS_QUERY)
                      .put("wait_for_completion_timeout", "0")));
    } catch (ResponseException e) {
      Assert.assertEquals(429, e.getResponse().getStatusLine().getStatusCode());
      Assert.assertTrue(errorBody(e).contains(SATURATED));
      return;
    }
    jobIds.add(async.getString("id"));
    JSONObject terminal = pollUntilTerminal(client(), async.getString("id"), 30_000);
    Assert.assertEquals("FAILED", terminal.getString("status"));
    Assert.assertTrue(terminal.getJSONObject("error").toString().contains(SATURATED));
  }

  /** Fills the complex pool's thread and queue slot with sync queries. */
  private void saturateComplexPool() throws Exception {
    String nodeId = localNodeId(client());
    busy.add(startSync(SLOW_QUERY));
    awaitPoolBusy(client(), nodeId, COMPLEX_POOL, 1, 0);
    busy.add(startSync(SLOW_QUERY));
    awaitPoolBusy(client(), nodeId, COMPLEX_POOL, 1, 1);
  }

  /** Starts a sync query without waiting for it. */
  private Cancellable startSync(String query) {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(new JSONObject().put("query", query).toString());
    return client()
        .performRequestAsync(
            request,
            new ResponseListener() {
              @Override
              public void onSuccess(Response response) {}

              @Override
              public void onFailure(Exception exception) {}
            });
  }

  private static String errorBody(ResponseException e) throws IOException {
    return getResponseBody(e.getResponse(), true);
  }
}
