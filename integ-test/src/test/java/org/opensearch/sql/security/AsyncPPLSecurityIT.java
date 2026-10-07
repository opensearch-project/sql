/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.INDEX;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.STREAMSTATS_QUERY;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.assertStopped;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.awaitPoolsIdle;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.indexSearchCount;

import java.io.IOException;
import java.util.List;
import org.apache.hc.core5.http.HttpHost;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.sql.job.QueryJobId;
import org.opensearch.sql.ppl.AsyncPPLTestHelpers;

/**
 * Async PPL ownership checks on the shared two-node secured {@code integTestWithSecurity} cluster.
 * Both users hold every async permission, so a 403 can only come from job ownership. Requests are
 * sent to the non-owner node to exercise owner forwarding.
 */
public class AsyncPPLSecurityIT extends SecurityTestBase {

  private static final String ALICE = "async_alice";
  private static final String BOB = "async_bob";
  private static final String ASYNC_PATH = "/_plugins/_async_query/";

  private RestClient owner;
  private RestClient peer;
  private String ownerNodeId;

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
    AsyncPPLTestHelpers.createIndex(client());
    createAsyncUser(ALICE);
    createAsyncUser(BOB);
    RestClient[] nodes = AsyncPPLTestHelpers.twoNodeClients(getClusterHosts(), this::nodeClient);
    owner = nodes[0];
    peer = nodes[1];
    ownerNodeId = AsyncPPLTestHelpers.localNodeId(owner);
  }

  @After
  public void tearDownFixture() throws Exception {
    try {
      if (owner != null) {
        awaitPoolsIdle(owner, ownerNodeId);
      }
    } finally {
      if (owner != null) {
        owner.close();
      }
      if (peer != null) {
        peer.close();
      }
    }
  }

  @Test
  public void otherUserCannotDeleteThroughForwardingAndOwnerStillCan() throws Exception {
    // Positive permission control: Bob's forwarded DELETE of an unknown owner-node id returns
    // 404, so a later 403 provably comes from job ownership, not a missing DELETE grant.
    String unknownId = QueryJobId.create(ownerNodeId).encode();
    assertNotFound(() -> asUser(peer, "DELETE", ASYNC_PATH + unknownId, BOB, null));

    long searchesBefore = indexSearchCount(owner);
    JSONObject submit = new JSONObject();
    submit.put("query", STREAMSTATS_QUERY);
    submit.put("wait_for_completion_timeout", "0");
    String queryId =
        new JSONObject(asUser(owner, "POST", "/_plugins/_ppl", ALICE, submit)).getString("id");
    List<String> runningPits = AsyncPPLTestHelpers.awaitRunning(owner, ownerNodeId, searchesBefore);

    assertForbidden(() -> asUser(peer, "DELETE", ASYNC_PATH + queryId, BOB, null));
    Assert.assertEquals(
        "RUNNING",
        new JSONObject(asUser(peer, "GET", ASYNC_PATH + queryId, ALICE, null)).getString("status"));

    // Capture the search counter right before Alice's DELETE so the post-cancel delta excludes
    // legitimate work performed while Bob was denied and Alice polled.
    long searchesAtDelete = indexSearchCount(owner);
    JSONObject deleted = new JSONObject(asUser(peer, "DELETE", ASYNC_PATH + queryId, ALICE, null));
    Assert.assertEquals("CANCELLED", deleted.getString("status"));
    assertStopped(owner, ownerNodeId, searchesAtDelete, runningPits);
    assertNotFound(() -> asUser(owner, "GET", ASYNC_PATH + queryId, ALICE, null));
  }

  private void createAsyncUser(String user) throws IOException {
    String role = user + "_role";
    createRoleWithPermissions(
        role,
        INDEX,
        new String[] {
          "cluster:admin/opensearch/ppl",
          "cluster:admin/opensearch/ql/async_query/result",
          "cluster:admin/opensearch/ql/async_query/delete"
        },
        new String[] {
          "indices:data/read/search*",
          "indices:admin/mappings/get",
          "indices:monitor/settings/get",
          "indices:data/read/point_in_time/create",
          "indices:data/read/point_in_time/delete",
          "indices:admin/resolve/index",
          "indices:data/read/field_caps*"
        });
    createUser(user, role);
  }

  private String asUser(RestClient node, String method, String path, String user, JSONObject body)
      throws IOException {
    Request request = new Request(method, path);
    if (body != null) {
      request.setJsonEntity(body.toString());
    }
    RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
    options.addHeader("Authorization", createBasicAuthHeader(user, STRONG_PASSWORD));
    request.setOptions(options);
    Response response = node.performRequest(request);
    return getResponseBody(response, true);
  }

  private static void assertForbidden(org.junit.function.ThrowingRunnable request) {
    ResponseException e = Assert.assertThrows(ResponseException.class, request);
    Assert.assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
  }

  private static void assertNotFound(org.junit.function.ThrowingRunnable request) {
    ResponseException e = Assert.assertThrows(ResponseException.class, request);
    Assert.assertEquals(404, e.getResponse().getStatusLine().getStatusCode());
  }

  private RestClient nodeClient(HttpHost host) {
    try {
      return buildClient(Settings.EMPTY, new HttpHost[] {host});
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
