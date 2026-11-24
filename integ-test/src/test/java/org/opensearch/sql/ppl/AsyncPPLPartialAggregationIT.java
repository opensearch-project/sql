/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.isIndexExist;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.getAsyncQuery;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.postPpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/** Running results of a pushed-down aggregation on async PPL responses (#5799). */
public class AsyncPPLPartialAggregationIT extends PPLIntegTestCase {

  private static final String INDEX = "async_partial_agg_fixture";
  private static final int SHARDS = 40;
  private static final int DOCS = 100_000;
  private static final int ATTEMPTS = 5;
  private static final long TIMEOUT_MILLIS = 60_000;
  // The script filter is pushed down too; it makes each shard slow enough to poll mid-run.
  private static final String QUERY =
      "source="
          + INDEX
          + " | where length(cast(n as string)) > 0 | stats max(n) as m, sum(n) as s, avg(n) as a";

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
    if (!isIndexExist(client(), INDEX)) {
      createIndex();
    }
  }

  @Test
  public void aggregationReturnsAMergeOfSomeShardsBeforeItFinishes() throws Exception {
    JSONObject sync = sync(QUERY);
    for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
      List<JSONObject> running = new ArrayList<>();
      JSONObject terminal = run(QUERY, running);
      assertMatchesSync(sync, terminal);
      List<JSONObject> withRows = running.stream().filter(r -> rows(r).length() > 0).toList();
      if (withRows.isEmpty()) {
        continue; // Finished before a poll saw a merge.
      }
      long finalSum = rows(terminal).getJSONArray(0).getLong(1);
      for (JSONObject response : withRows) {
        Assert.assertEquals(sync.getJSONArray("schema").toString(), schema(response));
        Assert.assertEquals(1, rows(response).length());
        Assert.assertTrue(rows(response).getJSONArray(0).getLong(1) <= finalSum);
      }
      return;
    }
    Assert.fail("no running response of " + ATTEMPTS + " runs had rows");
  }

  @Test
  public void turningTheMergeOffReturnsNoRowsUntilDone() throws Exception {
    String key = Key.QUERY_ASYNC_BATCHED_REDUCE_SIZE.getKeyValue();
    updateClusterSettings(new SQLIntegTestCase.ClusterSetting("persistent", key, "0"));
    try {
      List<JSONObject> running = new ArrayList<>();
      JSONObject terminal = run(QUERY, running);
      assertMatchesSync(sync(QUERY), terminal);
      running.forEach(r -> Assert.assertEquals(0, rows(r).length()));
    } finally {
      updateClusterSettings(new SQLIntegTestCase.ClusterSetting("persistent", key, null));
    }
  }

  private JSONObject run(String query, List<JSONObject> running) throws Exception {
    JSONObject submit =
        new JSONObject(
            postPpl(
                client(),
                new JSONObject().put("query", query).put("wait_for_completion_timeout", "0")));
    Assert.assertEquals("RUNNING", submit.getString("status"));
    running.add(submit);
    String id = submit.getString("id");
    long deadline = System.currentTimeMillis() + TIMEOUT_MILLIS;
    while (System.currentTimeMillis() < deadline) {
      JSONObject response = new JSONObject(getAsyncQuery(client(), id));
      if (!"RUNNING".equals(response.getString("status"))) {
        return response;
      }
      running.add(response);
      Thread.sleep(5);
    }
    throw new AssertionError("async job did not finish");
  }

  private JSONObject sync(String query) throws IOException {
    return new JSONObject(postPpl(client(), new JSONObject().put("query", query)));
  }

  private static void assertMatchesSync(JSONObject sync, JSONObject terminal) {
    Assert.assertEquals("SUCCEEDED", terminal.getString("status"));
    Assert.assertEquals(sync.getJSONArray("schema").toString(), schema(terminal));
    Assert.assertTrue(
        "async rows must equal sync rows: sync=" + sync + " async=" + terminal,
        sync.getJSONArray("datarows").similar(rows(terminal)));
  }

  private static String schema(JSONObject response) {
    return response.getJSONArray("schema").toString();
  }

  private static JSONArray rows(JSONObject response) {
    return response.getJSONArray("datarows");
  }

  /** No request cache: a cached aggregation returns before any merge. */
  private void createIndex() throws IOException {
    Request create = new Request("PUT", "/" + INDEX);
    create.setJsonEntity(
        "{\"settings\":{\"number_of_shards\":"
            + SHARDS
            + ",\"number_of_replicas\":0,\"requests.cache.enable\":false},\"mappings\":{\"properties\":{\"n\":{\"type\":\"long\"}}}}");
    client().performRequest(create);
    int chunk = 10_000;
    for (int offset = 0; offset < DOCS; offset += chunk) {
      StringBuilder bulk = new StringBuilder();
      for (int i = offset; i < Math.min(DOCS, offset + chunk); i++) {
        bulk.append("{\"index\":{}}\n{\"n\":").append(i).append("}\n");
      }
      Request request =
          new Request(
              "POST", "/" + INDEX + "/_bulk" + (offset + chunk >= DOCS ? "?refresh=true" : ""));
      request.setJsonEntity(bulk.toString());
      client().performRequest(request);
    }
  }
}
