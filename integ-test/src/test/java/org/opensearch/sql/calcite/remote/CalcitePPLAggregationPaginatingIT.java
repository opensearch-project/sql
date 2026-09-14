/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import java.util.Locale;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

public class CalcitePPLAggregationPaginatingIT extends CalcitePPLAggregationIT {
  private static final String REX_DEMO_INDEX = "ppl_partial_rex_demo";
  private static final int REX_DEMO_ROWS = 20_000;

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    setQueryBucketSize(2);
  }

  @After
  public void tearDown() throws Exception {
    resetQueryBucketSize();
    resetQuerySizeLimit();
    super.tearDown();
  }

  @Test
  public void testAsyncFullyPushedAggregationReportsProgressAndFinalRows() throws Exception {
    createRexDemoIndex();
    setQuerySizeLimit(REX_DEMO_ROWS);
    Request submit = new Request("POST", "/_plugins/_ppl");
    submit.setJsonEntity(
        new JSONObject()
            .put("query", String.format("source=%s | stats count() as total", REX_DEMO_INDEX))
            .put("wait_for_completion_timeout", "0ms")
            .put("keep_alive", "1m")
            .toString());
    long startNanos = System.nanoTime();
    JSONObject snapshot = perform(submit);
    reportSnapshot("submit", startNanos, snapshot);

    String jobId = snapshot.getString("id");

    double previousProgress = snapshot.getJSONObject("progress").getDouble("fraction_done");
    for (int attempt = 0;
        attempt < 500 && !"SUCCEEDED".equals(snapshot.getString("status"));
        attempt++) {
      Request poll =
          new Request(
              "GET",
              String.format(Locale.ROOT, "/_plugins/_ppl/jobs/%s?offset=0&count=1000", jobId));
      snapshot = perform(poll);

      Assert.assertEquals(jobId, snapshot.getString("id"));
      if (snapshot.has("update_mode")) {
        Assert.assertEquals("REPLACE", snapshot.getString("update_mode"));
      }
      double progress = snapshot.getJSONObject("progress").getDouble("fraction_done");
      Assert.assertTrue(progress >= previousProgress);
      Assert.assertTrue(progress >= 0D && progress <= 1D);
      if ("RUNNING".equals(snapshot.getString("status"))) {
        Assert.assertTrue(progress < 1D);
      }
      previousProgress = progress;
      reportSnapshot("aggregation-progress", startNanos, snapshot);
      Thread.sleep(5L);
    }

    reportSnapshot("final", startNanos, snapshot);
    Assert.assertEquals("SUCCEEDED", snapshot.getString("status"));
    Assert.assertEquals("REPLACE", snapshot.getString("update_mode"));
    Assert.assertEquals(1, snapshot.getInt("total"));
  }

  @Test
  public void testAsyncCoordinatorRexPollingLifecycle() throws Exception {
    createRexDemoIndex();
    setQuerySizeLimit(REX_DEMO_ROWS);
    String query =
        String.format(
            "source=%s | rex field=email \"(?<user>[^@]+)@(?<domain>.+)\""
                + " | fields event_id, email, user, domain",
            REX_DEMO_INDEX);

    Request explain = new Request("POST", "/_plugins/_ppl/_explain?mode=standard");
    explain.setJsonEntity(new JSONObject().put("query", query).toString());
    Response explainResponse = client().performRequest(explain);
    Assert.assertEquals(200, explainResponse.getStatusLine().getStatusCode());
    JSONObject explainJson = new JSONObject(getResponseBody(explainResponse, true));
    String physicalPlan = explainJson.getJSONObject("calcite").getString("physical");
    System.out.printf("PPL_REX_EXPLAIN physical=%s%n", physicalPlan);
    Assert.assertTrue(
        "Expected coordinator-side EnumerableCalc", physicalPlan.contains("EnumerableCalc"));
    Assert.assertTrue(
        "Expected coordinator-side REX_EXTRACT", physicalPlan.contains("REX_EXTRACT"));

    Request submit = new Request("POST", "/_plugins/_ppl");
    submit.setJsonEntity(
        new JSONObject()
            .put("query", query)
            .put("wait_for_completion_timeout", "0ms")
            .put("keep_alive", "1m")
            .toString());
    long startNanos = System.nanoTime();
    JSONObject snapshot = perform(submit);
    reportSnapshot("rex-submit", startNanos, snapshot);

    String jobId = snapshot.getString("id");
    boolean sawPartialRows = false;
    int firstPartialRows = 0;
    for (int attempt = 0;
        attempt < 1000 && !"SUCCEEDED".equals(snapshot.getString("status"));
        attempt++) {
      Request poll =
          new Request(
              "GET",
              String.format(Locale.ROOT, "/_plugins/_ppl/jobs/%s?offset=0&count=1000", jobId));
      snapshot = perform(poll);
      if (snapshot.has("update_mode")) {
        Assert.assertEquals("APPEND", snapshot.getString("update_mode"));
      }
      if ("RUNNING".equals(snapshot.getString("status"))
          && snapshot.getJSONArray("datarows").length() > 0) {
        if (!sawPartialRows) {
          firstPartialRows = snapshot.getJSONArray("datarows").length();
          reportSnapshot("rex-partial", startNanos, snapshot);
        }
        sawPartialRows = true;
      }
      Thread.sleep(5L);
    }

    reportSnapshot("rex-final", startNanos, snapshot);
    Assert.assertEquals("SUCCEEDED", snapshot.getString("status"));
    Assert.assertEquals("APPEND", snapshot.getString("update_mode"));
    Assert.assertTrue("Expected a non-empty rex partial snapshot", sawPartialRows);
    Assert.assertTrue(snapshot.getInt("total") > firstPartialRows);
    Assert.assertEquals(REX_DEMO_ROWS, snapshot.getInt("total"));
  }

  @Test
  public void testAsyncJobDeleteReleasesRetainedState() throws Exception {
    createRexDemoIndex();
    Request submit = new Request("POST", "/_plugins/_ppl");
    submit.setJsonEntity(
        new JSONObject()
            .put("query", String.format("source=%s | fields event_id", REX_DEMO_INDEX))
            .put("wait_for_completion_timeout", "0ms")
            .put("keep_alive", "1m")
            .toString());
    String jobId = perform(submit).getString("id");

    JSONObject deleted =
        perform(new Request("DELETE", String.format("/_plugins/_ppl/jobs/%s", jobId)));
    Assert.assertEquals(jobId, deleted.getString("id"));
    Assert.assertTrue(
        deleted.getString("status").equals("CANCELLED")
            || deleted.getString("status").equals("SUCCEEDED"));

    ResponseException error =
        Assert.assertThrows(
            ResponseException.class,
            () ->
                client()
                    .performRequest(
                        new Request("GET", String.format("/_plugins/_ppl/jobs/%s", jobId))));
    Assert.assertEquals(404, error.getResponse().getStatusLine().getStatusCode());
  }

  @Test
  public void testAsyncFastCompletionReturnsNormalResultWithoutJobId() throws Exception {
    Request submit = new Request("POST", "/_plugins/_ppl");
    submit.setJsonEntity(
        new JSONObject()
            .put("query", "source=opensearch-sql_test_index_account | head 1")
            .put("wait_for_completion_timeout", "30s")
            .put("keep_alive", "1m")
            .toString());

    JSONObject response = perform(submit);

    Assert.assertEquals("SUCCEEDED", response.getString("status"));
    Assert.assertFalse(response.has("id"));
    Assert.assertEquals(1D, response.getJSONObject("progress").getDouble("fraction_done"), 0D);
    Assert.assertEquals(1, response.getJSONArray("datarows").length());
  }

  private void createRexDemoIndex() throws Exception {
    Request delete = new Request("DELETE", "/" + REX_DEMO_INDEX);
    delete.addParameter("ignore_unavailable", "true");
    client().performRequest(delete);

    Request create = new Request("PUT", "/" + REX_DEMO_INDEX);
    create.setJsonEntity(
        """
        {
          "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
          },
          "mappings": {
            "properties": {
              "event_id": {"type": "integer"},
              "email": {"type": "keyword"}
            }
          }
        }
        """);
    Assert.assertEquals(200, client().performRequest(create).getStatusLine().getStatusCode());

    StringBuilder body = new StringBuilder();
    for (int eventId = 0; eventId < REX_DEMO_ROWS; eventId++) {
      body.append("{\"index\":{}}\n")
          .append(
              String.format(
                  Locale.ROOT,
                  "{\"event_id\":%d,\"email\":\"user%05d@example%02d.com\"}%n",
                  eventId,
                  eventId,
                  eventId % 100));
    }
    Request bulk = new Request("POST", "/" + REX_DEMO_INDEX + "/_bulk?refresh=true");
    bulk.setJsonEntity(body.toString());
    JSONObject bulkResponse = new JSONObject(getResponseBody(client().performRequest(bulk), true));
    Assert.assertFalse(bulkResponse.toString(), bulkResponse.getBoolean("errors"));
  }

  private JSONObject perform(Request request) throws Exception {
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return new JSONObject(getResponseBody(response, true));
  }

  private void reportSnapshot(String step, long startNanos, JSONObject snapshot) {
    JSONArray rows = snapshot.getJSONArray("datarows");
    JSONArray sampleRows = new JSONArray();
    for (int i = 0; i < Math.min(3, rows.length()); i++) {
      sampleRows.put(rows.get(i));
    }
    long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;
    System.out.printf(
        "PPL_PARTIAL_DEMO step=%s elapsed_ms=%d status=%s progress=%.4f rows=%d sample=%s%n",
        step,
        elapsedMillis,
        snapshot.getString("status"),
        snapshot.getJSONObject("progress").getDouble("fraction_done"),
        rows.length(),
        sampleRows);
  }
}
