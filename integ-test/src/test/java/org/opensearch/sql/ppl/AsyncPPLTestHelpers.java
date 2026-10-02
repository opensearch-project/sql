/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;

/**
 * Shared helpers for the async PPL integration tests ({@link AsyncPPLQueryLifecycleIT} and {@link
 * AsyncPPLMultiNodeRoutingIT}). Keeps the two IT classes free of duplicated boilerplate for POST /
 * GET / poll-until-terminal.
 */
final class AsyncPPLTestHelpers {

  static final String PPL_ENDPOINT = "/_plugins/_ppl";
  static final String ASYNC_QUERY_ENDPOINT = "/_plugins/_async_query/";

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
}
