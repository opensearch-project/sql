/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.ASYNC_QUERY_ENDPOINT;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.OVERFLOW_QUERY;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.STREAMSTATS_QUERY;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.assertNotFound;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.assertStopped;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.awaitPoolsIdle;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.awaitRunning;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.deleteAsyncQuery;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.getAsyncQuery;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.indexSearchCount;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.localNodeId;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.pollUntilTerminal;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.postPpl;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hc.core5.http.HttpHost;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.sql.job.QueryJobId;

/**
 * Multi-node IT for owner-node routing (issue #5765). Runs against the {@code asyncMultiNodeIT}
 * two-node test cluster.
 *
 * <ul>
 *   <li>submit lands on node A; the returned queryId encodes node A as owner;
 *   <li>GET on node B forwards to node A and returns the terminal snapshot with full schema + rows;
 *   <li>statement-level explain submitted on node A and fetched on node B returns the explain body
 *       produced by the owner's sync explain path;
 *   <li>DELETE on node B forwards to node A, returns the job's final status, and removes it from
 *       node A; an unknown id returns 404 across forwarding;
 *   <li>a long-running Calcite query cancelled through the non-owner node stops execution;
 *   <li>concurrent DELETEs on both nodes resolve to one 200 + one 404, and the single cancel drains
 *       the owner;
 *   <li>a job that fails naturally after the full scan reports FAILED on DELETE via the peer.
 * </ul>
 */
public class AsyncPPLMultiNodeRoutingIT extends PPLIntegTestCase {

  private RestClient nodeA;
  private RestClient nodeB;
  private String nodeAId;

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
    // Calcite reports field-not-found failures through the response listener so wait=0 returns
    // a RUNNING id and the subsequent GET carries the structured synchronous error body.
    // getClusterHosts() returns one HttpHost per bound address — a two-node cluster typically
    // yields four hosts of which hosts[0] and hosts[1] are the same node. Shared helper resolves
    // node ids per host and picks two with distinct ids; without this, owner-node forwarding never
    // triggers and the tests give a false pass.
    RestClient[] nodes = AsyncPPLTestHelpers.twoNodeClients(getClusterHosts(), this::nodeClient);
    nodeA = nodes[0];
    nodeB = nodes[1];
    nodeAId = localNodeId(nodeA);
  }

  @After
  public void closeNodeClients() throws Exception {
    try {
      // A running-cancel test that fails before its DELETE lands leaves a Calcite scan drawing
      // batches from the fixture index; wait for nodeA to drain so the next test can't take its
      // baseline against that orphan scan. If the drain still fails, raise it: a busy owner is
      // itself a defect worth surfacing, not something to swallow.
      if (nodeA != null && nodeAId != null) {
        awaitPoolsIdle(nodeA, nodeAId);
      }
    } finally {
      if (nodeA != null) {
        nodeA.close();
      }
      if (nodeB != null) {
        nodeB.close();
      }
    }
  }

  @Test
  public void get_forwardsFromNonOwnerNodeToOwner() throws Exception {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");

    JSONObject submitResponse = new JSONObject(postPpl(nodeA, body));
    Assert.assertTrue("submit response must carry queryId", submitResponse.has("id"));
    String queryId = submitResponse.getString("id");

    JSONObject fetched = pollUntilTerminal(nodeB, queryId, 30_000);
    Assert.assertEquals("SUCCEEDED", fetched.getString("status"));
    // Async GET formatter emits raw engine type ("long"), not the JDBC family ("bigint").
    verifySchema(fetched, schema("c", "long"));
    verifyDataRows(fetched, rows(1000));
    verifyNumOfRows(fetched, 1);
  }

  @Test
  public void getUnknownPplIdFromNonOwner_returns404() throws Exception {
    String fakeId = QueryJobId.create(nodeAId).encode();
    Request request = new Request("GET", ASYNC_QUERY_ENDPOINT + fakeId);
    ResponseException ex =
        Assert.assertThrows(ResponseException.class, () -> nodeB.performRequest(request));
    Assert.assertEquals(404, ex.getResponse().getStatusLine().getStatusCode());
  }

  @Test
  public void delete_forwardsFromNonOwnerNodeToOwner() throws Exception {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");
    String queryId = new JSONObject(postPpl(nodeA, body)).getString("id");
    Assert.assertEquals("SUCCEEDED", pollUntilTerminal(nodeA, queryId, 30_000).getString("status"));

    Response deleted = deleteAsyncQuery(nodeB, queryId);

    Assert.assertEquals(200, deleted.getStatusLine().getStatusCode());
    Assert.assertEquals(
        "SUCCEEDED", new JSONObject(getResponseBody(deleted, true)).getString("status"));
    assertNotFound(() -> getAsyncQuery(nodeA, queryId));
    assertNotFound(() -> getAsyncQuery(nodeB, queryId));
  }

  @Test
  public void deleteUnknownPplIdFromNonOwner_returns404() throws Exception {
    String fakeId = QueryJobId.create(nodeAId).encode();
    Request request = new Request("DELETE", ASYNC_QUERY_ENDPOINT + fakeId);
    ResponseException ex =
        Assert.assertThrows(ResponseException.class, () -> nodeB.performRequest(request));
    Assert.assertEquals(404, ex.getResponse().getStatusLine().getStatusCode());
  }

  @Test
  public void failed_forwardsFromNonOwnerNodeToOwner() throws Exception {
    // Capture the sync error body on nodeA so the parity assertion is anchored to a real sync
    // response rather than hard-coded fields.
    JSONObject syncBody = new JSONObject();
    syncBody.put("query", "source=" + TEST_INDEX_ACCOUNT + " | fields nonexistent_field");
    org.opensearch.client.Request syncRequest =
        new org.opensearch.client.Request("POST", AsyncPPLTestHelpers.PPL_ENDPOINT);
    syncRequest.setJsonEntity(syncBody.toString());
    org.opensearch.client.ResponseException syncEx =
        Assert.assertThrows(
            org.opensearch.client.ResponseException.class, () -> nodeA.performRequest(syncRequest));
    JSONObject syncError =
        new JSONObject(
                org.opensearch.sql.legacy.TestUtils.getResponseBody(syncEx.getResponse(), true))
            .getJSONObject("error");

    // Submit on nodeA with wait=0 so the runner fails after the submit returns.
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | fields nonexistent_field");
    body.put("wait_for_completion_timeout", "0");
    String queryId = new JSONObject(postPpl(nodeA, body)).getString("id");

    // Poll via nodeB (non-owner); request forwards to nodeA, which renders the full error body.
    JSONObject terminal = pollUntilTerminal(nodeB, queryId, 30_000);
    Assert.assertEquals("FAILED", terminal.getString("status"));
    Object errorField = terminal.get("error");
    Assert.assertTrue(
        "cross-node GET error must be a structured object", errorField instanceof JSONObject);
    JSONObject asyncError = (JSONObject) errorField;
    // Full deep equality — every field the sync body publishes (type, reason, details, code,
    // context, location, suggestion if present) must appear in the cross-node async body with
    // the same value. The sync and async paths render through the same SyncErrorReportRenderer.
    Assert.assertEquals(
        "cross-node async error must carry the same top-level keys as sync",
        syncError.keySet(),
        asyncError.keySet());
    Assert.assertTrue(
        "cross-node async error must deep-equal sync; sync=" + syncError + " async=" + asyncError,
        syncError.similar(asyncError));
  }

  @Test
  public void explain_forwardsFromNonOwnerNodeToOwner() throws Exception {
    JSONObject body = new JSONObject();
    body.put("query", "explain source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");

    String queryId = new JSONObject(postPpl(nodeA, body)).getString("id");

    long deadline = System.currentTimeMillis() + 30_000L;
    JSONObject explain = null;
    while (System.currentTimeMillis() < deadline) {
      String raw = getAsyncQuery(nodeB, queryId);
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
    Assert.assertNotNull("cross-node explain body never arrived", explain);
    Assert.assertTrue(
        "explain body must carry a plan tree", explain.has("calcite") || explain.has("root"));
  }

  @Test
  public void deleteOnPeerForwardsCancellationOfStreamstatsAndStopsExecution() throws Exception {
    // Uses default settings and executes on sql-complex-worker.
    AsyncPPLTestHelpers.createIndex(nodeA);
    long searchesBefore = indexSearchCount(nodeA);
    String queryId = submitAsync(nodeA, STREAMSTATS_QUERY);
    List<String> runningPits = awaitRunning(nodeA, nodeAId, searchesBefore);
    Assert.assertEquals(
        "RUNNING", new JSONObject(getAsyncQuery(nodeA, queryId)).getString("status"));

    Response deleted = deleteAsyncQuery(nodeB, queryId);
    Assert.assertEquals(200, deleted.getStatusLine().getStatusCode());
    Assert.assertEquals(
        "CANCELLED", new JSONObject(getResponseBody(deleted, true)).getString("status"));
    long searchesAfterDelete = indexSearchCount(nodeA);
    assertStopped(nodeA, nodeAId, searchesAfterDelete, runningPits);
    assertNotFound(() -> getAsyncQuery(nodeA, queryId));
    assertNotFound(() -> getAsyncQuery(nodeB, queryId));
    assertNotFound(() -> deleteAsyncQuery(nodeB, queryId));
  }

  @Test
  public void concurrentDeletesCancelOnceAndRemoveOnce() throws Exception {
    AsyncPPLTestHelpers.createIndex(nodeA);
    long searchesBefore = indexSearchCount(nodeA);
    String queryId = submitAsync(nodeA, STREAMSTATS_QUERY);
    List<String> runningPits = awaitRunning(nodeA, nodeAId, searchesBefore);

    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    List<Response> responses = new ArrayList<>();
    try {
      Future<Response> onOwner = executor.submit(() -> deleteWhenStarted(start, nodeA, queryId));
      Future<Response> onPeer = executor.submit(() -> deleteWhenStarted(start, nodeB, queryId));
      start.countDown();
      responses.add(onOwner.get(10, TimeUnit.SECONDS));
      responses.add(onPeer.get(10, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }

    List<Integer> codes = new ArrayList<>();
    for (Response response : responses) {
      int code = response.getStatusLine().getStatusCode();
      codes.add(code);
      if (code == 200) {
        Assert.assertEquals(
            "CANCELLED", new JSONObject(getResponseBody(response, true)).getString("status"));
      }
    }
    codes.sort(null);
    Assert.assertEquals(List.of(200, 404), codes);
    long searchesAfterDelete = indexSearchCount(nodeA);
    assertStopped(nodeA, nodeAId, searchesAfterDelete, runningPits);
    assertNotFound(() -> getAsyncQuery(nodeA, queryId));
  }

  @Test
  public void deleteOfFailedJobReturnsFailedAndRemovesIt() throws Exception {
    AsyncPPLTestHelpers.createIndex(nodeA);
    String queryId = submitAsync(nodeA, OVERFLOW_QUERY);
    JSONObject terminal = pollUntilTerminal(nodeA, queryId, 30_000);
    Assert.assertEquals("FAILED", terminal.getString("status"));
    JSONObject error = terminal.getJSONObject("error");
    Assert.assertEquals("ArithmeticException", error.getString("type"));
    Assert.assertTrue(
        "FAILED body must identify the overflow: " + terminal,
        error.getString("details").toLowerCase(java.util.Locale.ROOT).contains("overflow"));

    Response deleted = deleteAsyncQuery(nodeB, queryId);
    Assert.assertEquals(200, deleted.getStatusLine().getStatusCode());
    Assert.assertEquals(
        "FAILED", new JSONObject(getResponseBody(deleted, true)).getString("status"));
    assertNotFound(() -> getAsyncQuery(nodeA, queryId));
    assertNotFound(() -> deleteAsyncQuery(nodeB, queryId));
  }

  private String submitAsync(RestClient client, String query) throws IOException {
    JSONObject body = new JSONObject();
    body.put("query", query);
    body.put("wait_for_completion_timeout", "0");
    return new JSONObject(postPpl(client, body)).getString("id");
  }

  private static Response deleteWhenStarted(CountDownLatch start, RestClient node, String queryId)
      throws Exception {
    start.await();
    try {
      return deleteAsyncQuery(node, queryId);
    } catch (ResponseException e) {
      return e.getResponse();
    }
  }

  private RestClient nodeClient(HttpHost host) {
    try {
      return buildClient(Settings.EMPTY, new HttpHost[] {host});
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
