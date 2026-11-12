/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.deleteAsyncQuery;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.pollUntilTerminal;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.postPpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.common.setting.Settings.Key;

/**
 * End-to-end IT for {@code plugins.query.async.max_retained_results}, the limit on asynchronous PPL
 * results a node retains for polling.
 *
 * <p>Results left by earlier test classes may still be retained on the node. With a limit of one, a
 * submit is refused either at once, because such a result fills the limit, or after one result of
 * this test fills it.
 */
public class AsyncPPLRetainedResultLimitIT extends PPLIntegTestCase {

  private static final String MAX_RESULTS = Key.QUERY_ASYNC_MAX_RETAINED_RESULTS.getKeyValue();
  private static final String QUERY = "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c";
  private static final long POLL_TIMEOUT_MILLIS = 30_000;

  private final List<String> jobIds = new ArrayList<>();

  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    enableCalcite();
  }

  @After
  public void cleanUp() throws IOException {
    setLimit(null);
    for (String id : jobIds) {
      try {
        deleteAsyncQuery(client(), id);
      } catch (ResponseException e) {
        // Already deleted or evicted.
      }
    }
    jobIds.clear();
  }

  @Test
  public void submitIsRejectedWith429WhileRetainedResultsReachTheLimit() throws Exception {
    setLimit(1);
    ResponseException rejected;
    try {
      String id = submitRetained();
      // The retained result fills the limit once the query finishes, even after GET fetches it.
      pollUntilTerminal(client(), id, POLL_TIMEOUT_MILLIS);
      rejected = Assert.assertThrows(ResponseException.class, this::submitRetained);
    } catch (ResponseException e) {
      rejected = e;
    }

    Assert.assertEquals(429, rejected.getResponse().getStatusLine().getStatusCode());
    String body = getResponseBody(rejected.getResponse(), true);
    Assert.assertTrue(body, body.contains(MAX_RESULTS));
  }

  /** Submits {@link #QUERY} with a zero wait and returns the id of the retained job. */
  private String submitRetained() throws IOException {
    JSONObject body = new JSONObject().put("query", QUERY).put("wait_for_completion_timeout", "0");
    JSONObject response = new JSONObject(postPpl(client(), body));
    String id = response.getString("id");
    jobIds.add(id);
    return id;
  }

  private void setLimit(Integer limit) throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    request.setJsonEntity(
        new JSONObject()
            .put(
                "transient",
                new JSONObject().put(MAX_RESULTS, limit == null ? JSONObject.NULL : limit))
            .toString());
    client().performRequest(request);
  }
}
