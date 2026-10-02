/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.getAsyncQuery;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.pollUntilTerminal;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.postPpl;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.apache.hc.core5.http.HttpHost;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.RestClient;

/**
 * Multi-node IT for owner-node routing (issue #5765).
 *
 * <p>Runs against the {@code asyncMultiNodeIT} test cluster (two nodes). Verifies:
 *
 * <ul>
 *   <li>submit lands on node A; the returned queryId encodes node A as owner;
 *   <li>GET on node B forwards to node A and returns the terminal snapshot with full schema + rows;
 *   <li>statement-level explain submitted on node A and fetched on node B returns the explain body
 *       produced by the owner's sync explain path.
 * </ul>
 *
 * <p>Each request is pinned to a specific node by constructing a dedicated {@link RestClient} for
 * that node's HTTP address. The base {@link PPLIntegTestCase#client()} would round-robin across the
 * cluster hosts, which defeats the purpose of the test.
 */
public class AsyncPPLMultiNodeRoutingIT extends PPLIntegTestCase {

  private RestClient nodeA;
  private RestClient nodeB;

  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);

    // getClusterHosts() returns one HttpHost per bound address — in test clusters each node binds
    // both [::1] AND 127.0.0.1, so a two-node cluster yields four hosts of which hosts[0] and
    // hosts[1] are typically the *same* node. Resolve node ids per host and pick two with distinct
    // ids, otherwise owner-node forwarding never triggers and the test gives a false pass.
    java.util.List<org.apache.hc.core5.http.HttpHost> hosts = getClusterHosts();
    HttpHost first = hosts.get(0);
    String firstNodeId = probeNodeId(first);
    HttpHost second = null;
    for (int i = 1; i < hosts.size(); i++) {
      HttpHost candidate = hosts.get(i);
      if (!probeNodeId(candidate).equals(firstNodeId)) {
        second = candidate;
        break;
      }
    }
    if (second == null) {
      Assert.fail(
          "AsyncPPLMultiNodeRoutingIT needs two distinct nodes; all cluster hosts resolve to "
              + firstNodeId);
    }
    nodeA = RestClient.builder(first).build();
    nodeB = RestClient.builder(second).build();
  }

  @After
  public void closeNodeClients() throws IOException {
    if (nodeA != null) {
      nodeA.close();
    }
    if (nodeB != null) {
      nodeB.close();
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
    // A QueryJobId whose node id matches nodeA (owner-encoded) but the context id doesn't exist.
    String fakeId = org.opensearch.sql.job.QueryJobId.create(nodeIdOf(nodeA)).encode();
    org.opensearch.client.Request request =
        new org.opensearch.client.Request("GET", AsyncPPLTestHelpers.ASYNC_QUERY_ENDPOINT + fakeId);
    org.opensearch.client.ResponseException ex =
        Assert.assertThrows(
            org.opensearch.client.ResponseException.class, () -> nodeB.performRequest(request));
    int code = ex.getResponse().getStatusLine().getStatusCode();
    // Owner's QueryJobNotFoundException must translate to a transport-serializable 404 so
    // forwarding doesn't drop it to 500.
    Assert.assertEquals("expected 404 across owner-node forwarding, got " + code, 404, code);
  }

  private static String nodeIdOf(org.opensearch.client.RestClient client) throws IOException {
    org.opensearch.client.Response response =
        client.performRequest(new org.opensearch.client.Request("GET", "/_nodes/_local"));
    JSONObject body =
        new JSONObject(org.opensearch.sql.legacy.TestUtils.getResponseBody(response, true));
    JSONObject nodes = body.getJSONObject("nodes");
    return nodes.keys().next();
  }

  private static String probeNodeId(HttpHost host) throws IOException {
    try (RestClient client = RestClient.builder(host).build()) {
      return nodeIdOf(client);
    }
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
}
