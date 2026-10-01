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

    HttpHost[] hosts = getClusterHosts().toArray(new HttpHost[0]);
    if (hosts.length < 2) {
      Assert.fail("AsyncPPLMultiNodeRoutingIT requires a two-node cluster; got " + hosts.length);
    }
    nodeA = RestClient.builder(hosts[0]).build();
    nodeB = RestClient.builder(hosts[1]).build();
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
