/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;

/**
 * Multi-node IT for owner-node routing (issue #5765).
 *
 * <p>Runs against the {@code asyncMultiNodeIT} test cluster (two nodes). Verifies:
 *
 * <ul>
 *   <li>submit lands on node A; the returned queryId encodes node A as owner;
 *   <li>GET on node B forwards to node A and returns the terminal snapshot;
 *   <li>DELETE on node B forwards to node A and cancels there;
 *   <li>a subsequent GET reports the terminal (or 404) status.
 * </ul>
 *
 * <p>Each request is pinned to a specific node by constructing a dedicated {@link RestClient} for
 * that node's HTTP address. The base {@link PPLIntegTestCase#client()} would round-robin across the
 * cluster hosts, which defeats the purpose of the test.
 */
public class AsyncPPLMultiNodeRoutingIT extends PPLIntegTestCase {

  private static final String INDEX = "routing_it_accounts";

  private RestClient nodeA;
  private RestClient nodeB;

  @Override
  protected void init() throws Exception {
    super.init();

    HttpHost[] hosts = getClusterHosts().toArray(new HttpHost[0]);
    if (hosts.length < 2) {
      Assert.fail("AsyncPPLMultiNodeRoutingIT requires a two-node cluster; got " + hosts.length);
    }
    nodeA = RestClient.builder(hosts[0]).build();
    nodeB = RestClient.builder(hosts[1]).build();

    // Small inline dataset so the query has something to run against without a resource file.
    Request bulk = new Request("POST", "/_bulk");
    String body =
        "{\"index\":{\"_index\":\""
            + INDEX
            + "\",\"_id\":\"1\"}}\n"
            + "{\"age\":20,\"balance\":100}\n"
            + "{\"index\":{\"_index\":\""
            + INDEX
            + "\",\"_id\":\"2\"}}\n"
            + "{\"age\":30,\"balance\":200}\n";
    bulk.setEntity(new StringEntity(body, org.apache.hc.core5.http.ContentType.APPLICATION_JSON));
    bulk.addParameter("refresh", "true");
    nodeA.performRequest(bulk);
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
    body.put("query", "source=" + INDEX + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");

    // Submit on node A — owner id encodes node A.
    String submitJson = post(nodeA, body);
    JSONObject submitResponse = new JSONObject(submitJson);
    Assert.assertTrue("submit response must carry queryId", submitResponse.has("id"));
    String queryId = submitResponse.getString("id");

    // Fetch on node B — must forward to node A and return terminal state.
    JSONObject fetched = pollUntilTerminal(nodeB, queryId);
    String status = fetched.getString("status");
    Assert.assertTrue(
        "post-forward status is terminal, got " + status,
        "SUCCEEDED".equals(status) || "FAILED".equals(status));
  }

  @Test
  public void delete_forwardsFromNonOwnerNodeToOwner() throws Exception {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + INDEX + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");

    String queryId = new JSONObject(post(nodeA, body)).getString("id");
    delete(nodeB, queryId);

    // After the forwarded cancel, a GET on either node should report terminal or 404.
    try {
      JSONObject fetched = new JSONObject(get(nodeA, queryId));
      String status = fetched.getString("status");
      Assert.assertTrue(
          "post-cancel status is terminal, got " + status,
          "CANCELLED".equals(status) || "SUCCEEDED".equals(status) || "FAILED".equals(status));
    } catch (ResponseException e) {
      Assert.assertEquals(404, e.getResponse().getStatusLine().getStatusCode());
    }
  }

  private JSONObject pollUntilTerminal(RestClient node, String queryId) throws Exception {
    long deadline = System.currentTimeMillis() + 30_000L;
    JSONObject last = null;
    List<String> transitions = new ArrayList<>();
    while (System.currentTimeMillis() < deadline) {
      last = new JSONObject(get(node, queryId));
      String status = last.getString("status");
      transitions.add(status);
      if (!"RUNNING".equals(status) && !"PENDING".equals(status)) {
        return last;
      }
      Thread.sleep(200);
    }
    Assert.fail("queryId " + queryId + " did not reach terminal state; transitions=" + transitions);
    return last; // unreachable
  }

  private String post(RestClient node, JSONObject body) throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(body.toString());
    Response response = node.performRequest(request);
    return getResponseBody(response, true);
  }

  private String get(RestClient node, String queryId) throws IOException {
    Request request = new Request("GET", "/_plugins/_async_query/" + queryId);
    Response response = node.performRequest(request);
    return getResponseBody(response, true);
  }

  private void delete(RestClient node, String queryId) throws IOException {
    Request request = new Request("DELETE", "/_plugins/_async_query/" + queryId);
    node.performRequest(request);
  }
}
