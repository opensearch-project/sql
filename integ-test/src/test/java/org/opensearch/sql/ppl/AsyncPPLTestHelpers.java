/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import org.apache.hc.core5.http.HttpHost;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.function.ThrowingRunnable;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;

/**
 * Shared helpers for the async PPL integration tests ({@link AsyncPPLQueryLifecycleIT}, {@link
 * AsyncPPLMultiNodeRoutingIT}, and {@code AsyncPPLSecurityIT}). Covers lifecycle plumbing and the
 * cancellation fixture: a dedicated bulk-loaded index, a streamstats workload, running proof, and
 * the stop oracle.
 */
public final class AsyncPPLTestHelpers {

  static final String PPL_ENDPOINT = "/_plugins/_ppl";
  static final String ASYNC_QUERY_ENDPOINT = "/_plugins/_async_query/";

  /** Dedicated index used by the cancellation fixture; isolated from the shared datasets. */
  public static final String INDEX = "async_cancel_fixture";

  /** streamstats keeps the scan serial; the aggregate forces the planner to keep the window. */
  public static final String STREAMSTATS_QUERY =
      "source="
          + INDEX
          + " | streamstats count() as running_count | stats max(running_count) as total";

  /** Runs the full streamstats scan, then overflows a bigint add at projection time. */
  static final String OVERFLOW_QUERY =
      STREAMSTATS_QUERY + " | eval bad = 9223372036854775807 + total | fields bad";

  private static final int DOCS = 10_000;
  private static final int MAX_RESULT_WINDOW = 10;
  private static final long STOP_DEADLINE_MILLIS = 5_000;
  private static final long START_DEADLINE_MILLIS = 10_000;
  private static final long RUNNING_MIN_PROGRESS = 10;
  private static final long RUNNING_MAX_PROGRESS = DOCS / MAX_RESULT_WINDOW / 2;
  private static final int MAX_SEARCHES_AFTER_CANCEL = 20;
  private static final List<String> SQL_POOLS =
      List.of("sql-worker", "sql-complex-worker", "sql_background_io");

  private AsyncPPLTestHelpers() {}

  /** POST {@code /_plugins/_ppl} with the given JSON body; returns the raw response body. */
  static String postPpl(RestClient client, JSONObject body) throws IOException {
    return postPpl(client, PPL_ENDPOINT, body);
  }

  static String postPpl(RestClient client, String endpoint, JSONObject body) throws IOException {
    Request request = new Request("POST", endpoint);
    request.setJsonEntity(body.toString());
    Response response = client.performRequest(request);
    return getResponseBody(response, true);
  }

  /** GET {@code /_plugins/_async_query/{id}}; returns the raw response body. */
  static String getAsyncQuery(RestClient client, String queryId) throws IOException {
    Request request = new Request("GET", ASYNC_QUERY_ENDPOINT + queryId);
    Response response = client.performRequest(request);
    return getResponseBody(response, true);
  }

  /** DELETE {@code /_plugins/_async_query/{id}}; returns the response. */
  static Response deleteAsyncQuery(RestClient client, String queryId) throws IOException {
    return client.performRequest(new Request("DELETE", ASYNC_QUERY_ENDPOINT + queryId));
  }

  /** Asserts that {@code request} fails with HTTP 404. */
  static void assertNotFound(ThrowingRunnable request) {
    ResponseException ex = Assert.assertThrows(ResponseException.class, request);
    Assert.assertEquals(404, ex.getResponse().getStatusLine().getStatusCode());
  }

  /** Resolves the id of the node that serves {@code client}'s requests. */
  public static String localNodeId(RestClient client) throws IOException {
    Response response = client.performRequest(new Request("GET", "/_nodes/_local"));
    return new JSONObject(getResponseBody(response, true)).getJSONObject("nodes").keys().next();
  }

  /** Polls GET until the job reaches a terminal state or the timeout expires. */
  static JSONObject pollUntilTerminal(RestClient client, String queryId, long timeoutMillis)
      throws Exception {
    long deadline = System.currentTimeMillis() + timeoutMillis;
    JSONObject last = null;
    while (System.currentTimeMillis() < deadline) {
      last = new JSONObject(getAsyncQuery(client, queryId));
      String status = last.getString("status");
      if (!"RUNNING".equals(status) && !"PENDING".equals(status)) {
        return last;
      }
      Thread.sleep(200);
    }
    Assert.fail(
        "async job ["
            + queryId
            + "] did not reach terminal state within "
            + timeoutMillis
            + "ms. last="
            + last);
    return last; // unreachable
  }

  /** Bulk-loads {@link #INDEX} with {@value #DOCS} docs if it isn't already present. */
  public static void createIndex(RestClient admin) throws IOException {
    if (admin.performRequest(new Request("HEAD", "/" + INDEX)).getStatusLine().getStatusCode()
        == 200) {
      return;
    }
    Request create = new Request("PUT", "/" + INDEX);
    create.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0,"
                + "\"max_result_window\":%d},"
                + "\"mappings\":{\"properties\":{\"n\":{\"type\":\"integer\"}}}}",
            MAX_RESULT_WINDOW));
    admin.performRequest(create);
    int chunk = 1_000;
    for (int offset = 0; offset < DOCS; offset += chunk) {
      StringBuilder bulk = new StringBuilder(chunk * 30);
      for (int i = 0; i < chunk && offset + i < DOCS; i++) {
        bulk.append("{\"index\":{}}\n{\"n\":").append(offset + i).append("}\n");
      }
      Request bulkRequest =
          new Request(
              "POST", "/" + INDEX + "/_bulk" + (offset + chunk >= DOCS ? "?refresh=true" : ""));
      bulkRequest.setJsonEntity(bulk.toString());
      admin.performRequest(bulkRequest);
    }
  }

  /** Returns the total number of searches the owner has executed against {@link #INDEX}. */
  public static long indexSearchCount(RestClient admin) throws IOException {
    JSONObject body =
        new JSONObject(
            getResponseBody(
                admin.performRequest(new Request("GET", "/" + INDEX + "/_stats")), true));
    JSONObject stats = body.getJSONObject("indices").getJSONObject(INDEX).getJSONObject("total");
    return stats.getJSONObject("search").getLong("query_total");
  }

  /**
   * Running proof: the query must be active on {@code sql-complex-worker}, have an open PIT, and
   * have made search progress inside the running window. Returns the PIT ids seen while running.
   */
  public static List<String> awaitRunning(
      RestClient owner, String ownerNodeId, long searchesBeforeSubmit) throws Exception {
    long deadline = System.currentTimeMillis() + START_DEADLINE_MILLIS;
    while (System.currentTimeMillis() <= deadline) {
      Map<String, Map<String, Integer>> pools = poolStats(owner, ownerNodeId);
      long progress = indexSearchCount(owner) - searchesBeforeSubmit;
      if (pools.get("sql-complex-worker").get("active") > 0
          && progress >= RUNNING_MIN_PROGRESS
          && progress <= RUNNING_MAX_PROGRESS) {
        // Only probe the native PIT listing once worker activity and search progress show the
        // query is actually running, so this check can't race the engine finishing PIT setup
        // and get a transient 500 from the native endpoint for an in-flight context id.
        List<String> pits = openPits(owner);
        if (!pits.isEmpty()) {
          return pits;
        }
      }
      Thread.sleep(10);
    }
    Assert.fail(
        "query never reached a running state on sql-complex-worker: pools="
            + poolStats(owner, ownerNodeId)
            + " searchesBeforeSubmit="
            + searchesBeforeSubmit
            + " now="
            + indexSearchCount(owner)
            + " pits="
            + openPits(owner));
    return null; // unreachable
  }

  /** Waits until every SQL pool's active + queue reaches zero. */
  public static void awaitPoolsIdle(RestClient owner, String ownerNodeId) throws Exception {
    long deadline = System.currentTimeMillis() + STOP_DEADLINE_MILLIS;
    Map<String, Map<String, Integer>> pools = poolStats(owner, ownerNodeId);
    while (!poolsIdle(pools)) {
      if (System.currentTimeMillis() > deadline) {
        Assert.fail(
            "sql pools never went idle on "
                + ownerNodeId
                + " within "
                + STOP_DEADLINE_MILLIS
                + "ms: "
                + pools);
      }
      Thread.sleep(50);
      pools = poolStats(owner, ownerNodeId);
    }
  }

  /**
   * Waits for every SQL pool to drain, asserts the search delta since {@code searchesAtDelete}
   * stayed within {@link #MAX_SEARCHES_AFTER_CANCEL}, and confirms the tracked PITs were released.
   * {@code searchesAtDelete} must be captured immediately before the DELETE.
   */
  public static void assertStopped(
      RestClient owner, String ownerNodeId, long searchesAtDelete, List<String> pitsSeenRunning)
      throws Exception {
    awaitPoolsIdle(owner, ownerNodeId);
    long delta = indexSearchCount(owner) - searchesAtDelete;
    Assert.assertTrue(
        "cancelled query must not keep scanning past "
            + MAX_SEARCHES_AFTER_CANCEL
            + " searches: delta="
            + delta,
        delta <= MAX_SEARCHES_AFTER_CANCEL);
    assertPitsClosed(owner, pitsSeenRunning);
  }

  /**
   * Returns clients pinned to two distinct nodes. Test clusters bind each node to several
   * addresses, so hosts are deduplicated by the node id they report.
   */
  public static RestClient[] twoNodeClients(
      List<HttpHost> hosts, Function<HttpHost, RestClient> builder) throws IOException {
    RestClient first = builder.apply(hosts.get(0));
    String firstId = localNodeId(first);
    for (int i = 1; i < hosts.size(); i++) {
      RestClient candidate = builder.apply(hosts.get(i));
      if (!localNodeId(candidate).equals(firstId)) {
        return new RestClient[] {first, candidate};
      }
      candidate.close();
    }
    first.close();
    throw new AssertionError("async fixture tests need two distinct nodes: " + hosts);
  }

  private static List<String> openPits(RestClient admin) throws IOException {
    // The native /_search/point_in_time/_all endpoint occasionally throws 500 when a PIT handle
    // is being released at the same time as the list is built; retry briefly so the oracle still
    // sees the real list rather than treating a transient 500 as "no open PITs" or failing on
    // first contact. Any other status is propagated unchanged, so 403/404 keep their meaning.
    ResponseException last = null;
    for (int attempt = 0; attempt < 10; attempt++) {
      try {
        JSONObject body =
            new JSONObject(
                getResponseBody(
                    admin.performRequest(new Request("GET", "/_search/point_in_time/_all")), true));
        List<String> ids = new ArrayList<>();
        if (body.has("pits")) {
          JSONArray arr = body.getJSONArray("pits");
          for (int i = 0; i < arr.length(); i++) {
            String id = arr.getJSONObject(i).optString("pit_id", null);
            if (id != null) {
              ids.add(id);
            }
          }
        }
        return ids;
      } catch (ResponseException e) {
        if (e.getResponse().getStatusLine().getStatusCode() != 500) {
          throw e;
        }
        last = e;
        try {
          Thread.sleep(50);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException(ie);
        }
      }
    }
    throw last;
  }

  private static void assertPitsClosed(RestClient admin, List<String> pitsSeenRunning)
      throws Exception {
    long deadline = System.currentTimeMillis() + STOP_DEADLINE_MILLIS;
    while (true) {
      List<String> stillOpen = openPits(admin);
      boolean anyLeaked = stillOpen.stream().anyMatch(pitsSeenRunning::contains);
      if (!anyLeaked) {
        return;
      }
      if (System.currentTimeMillis() > deadline) {
        Assert.fail(
            "PITs opened by the cancelled query were not released: seen="
                + pitsSeenRunning
                + " stillOpen="
                + stillOpen);
      }
      Thread.sleep(50);
    }
  }

  private static Map<String, Map<String, Integer>> poolStats(RestClient node, String nodeId)
      throws IOException {
    JSONObject raw =
        new JSONObject(
            getResponseBody(
                node.performRequest(new Request("GET", "/_nodes/" + nodeId + "/stats/thread_pool")),
                true));
    JSONObject pools =
        raw.getJSONObject("nodes").getJSONObject(nodeId).getJSONObject("thread_pool");
    Map<String, Map<String, Integer>> snapshot = new LinkedHashMap<>();
    for (String pool : SQL_POOLS) {
      JSONObject p = pools.optJSONObject(pool);
      Map<String, Integer> counts = new LinkedHashMap<>();
      counts.put("active", p == null ? 0 : p.getInt("active"));
      counts.put("queue", p == null ? 0 : p.getInt("queue"));
      snapshot.put(pool, counts);
    }
    return snapshot;
  }

  private static boolean poolsIdle(Map<String, Map<String, Integer>> pools) {
    for (Map<String, Integer> counts : pools.values()) {
      if (counts.get("active") > 0 || counts.get("queue") > 0) {
        return false;
      }
    }
    return true;
  }
}
