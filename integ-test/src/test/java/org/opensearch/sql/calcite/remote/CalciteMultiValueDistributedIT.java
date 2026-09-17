/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;

/**
 * Multi-shard (distributed) integration tests for multi-value keyword fields, driven through the
 * SQL plugin PPL endpoint against the analytics engine. Complements the single-shard {@link
 * CalciteMultiValueKeywordOperatorIT} by exercising the coordinator reduce / cross-shard FFI paths
 * that a single shard never triggers:
 *
 * <ul>
 *   <li>Two-phase (PARTIAL on shard, FINAL on coordinator) aggregation over a LIST group key.
 *   <li>Backend routing assertions via the profile API (AE {@code datafusion} vs Lucene
 *       delegation).
 *   <li>Cross-shard projection of a stored LIST column (exercises the Utf8/Utf8View LIST-child
 *       schema boundary fixed by opensearch-project/OpenSearch#23040).
 * </ul>
 *
 * <p>Assertions are order-independent where result ordering is not query-guaranteed (rows are
 * sorted before comparison) and exact otherwise.
 */
public class CalciteMultiValueDistributedIT extends OpenSearchRestTestCase {

  private static final String INDEX = "mv_kw_dist";
  private static final int SHARDS = 2;

  private void ensureSetup() throws IOException {
    enableCalcite();
    provision();
  }

  private void enableCalcite() throws IOException {
    Request req = new Request("PUT", "/_cluster/settings");
    req.setJsonEntity("{\"persistent\":{\"plugins.calcite.enabled\":true}}");
    client().performRequest(req);
  }

  private void provision() throws IOException {
    try {
      client().performRequest(new Request("DELETE", "/" + INDEX));
    } catch (Exception ignored) {
    }
    String mapping =
        String.format(
            Locale.ROOT,
            "{\"settings\":{\"number_of_shards\":%d,\"number_of_replicas\":0,"
                + "\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"]},"
                + "\"mappings\":{\"properties\":{"
                + "\"id\":{\"type\":\"keyword\"},"
                + "\"tags\":{\"type\":\"keyword\",\"multi_value\":true}}}}",
            SHARDS);
    Request create = new Request("PUT", "/" + INDEX);
    create.setJsonEntity(mapping);
    client().performRequest(create);

    Request health = new Request("GET", "/_cluster/health/" + INDEX);
    health.addParameter("wait_for_status", "green");
    health.addParameter("timeout", "30s");
    client().performRequest(health);

    // Six docs so that, with 2 shards and default routing, both shards receive documents and the
    // group-by aggregation must reduce partial states across shards.
    //   d1 [prod]        d2 [blue]         d3 [prod, blue]
    //   d4 [green, prod] d5 [blue, green]  d6 [prod]
    String bulk =
        "{\"index\":{}}\n{\"id\":\"d1\",\"tags\":[\"prod\"]}\n"
            + "{\"index\":{}}\n{\"id\":\"d2\",\"tags\":[\"blue\"]}\n"
            + "{\"index\":{}}\n{\"id\":\"d3\",\"tags\":[\"prod\",\"blue\"]}\n"
            + "{\"index\":{}}\n{\"id\":\"d4\",\"tags\":[\"green\",\"prod\"]}\n"
            + "{\"index\":{}}\n{\"id\":\"d5\",\"tags\":[\"blue\",\"green\"]}\n"
            + "{\"index\":{}}\n{\"id\":\"d6\",\"tags\":[\"prod\"]}\n";
    Request bulkReq = new Request("POST", "/" + INDEX + "/_bulk");
    bulkReq.addParameter("refresh", "true");
    bulkReq.setJsonEntity(bulk);
    client().performRequest(bulkReq);
    client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
  }

  // ==================== multi-shard partial aggregation ====================

  @Test
  public void testMultiShardMvexpandGroupByReducesAcrossShards() throws IOException {
    ensureSetup();
    // Per-element counts across all 6 docs (mvexpand does not dedup within a doc):
    //   blue:  d2, d3, d5            = 3
    //   green: d4, d5                = 2
    //   prod:  d1, d3, d4, d6        = 4
    JSONObject r =
        query("source = " + INDEX + " | mvexpand tags | stats count() as c by tags | sort tags");
    // stats output column order is (count, group-key): c, tags.
    assertRowsUnordered(r, List.of("3,blue", "2,green", "4,prod"), "c", "tags");
  }

  @Test
  public void testMultiShardPartialAggProfileHasPartialAndFinalStages() throws IOException {
    ensureSetup();
    JSONObject result =
        executeWithProfile(
            "source = " + INDEX + " | mvexpand tags | stats count() as c by tags",
            "/_plugins/_ppl");
    JSONArray stages = result.getJSONObject("profile").getJSONObject("plan").getJSONArray("stages");

    boolean sawShardFragment = false;
    boolean sawCoordinatorReduce = false;
    int maxTasksInAStage = 0;
    for (int i = 0; i < stages.length(); i++) {
      JSONObject stage = stages.getJSONObject(i);
      assertEquals("stage succeeded", "SUCCEEDED", stage.getString("state"));
      String type = stage.optString("execution_type", "");
      if ("SHARD_FRAGMENT".equals(type)) sawShardFragment = true;
      if ("COORDINATOR_REDUCE".equals(type)) sawCoordinatorReduce = true;
      maxTasksInAStage = Math.max(maxTasksInAStage, stage.getJSONArray("tasks").length());
    }
    assertTrue("has a SHARD_FRAGMENT stage", sawShardFragment);
    assertTrue("has a COORDINATOR_REDUCE stage (cross-shard reduce)", sawCoordinatorReduce);
    // A 2-shard index dispatches the shard fragment to both shards.
    assertEquals("shard fragment ran on both shards", SHARDS, maxTasksInAStage);
  }

  // ==================== backend routing via profile ====================

  @Test
  public void testMultiValueGroupByRoutesToAnalyticsEngine() throws IOException {
    ensureSetup();
    // A group-by over a LIST column requires element expansion, which only the AE (datafusion)
    // supports -> the plan must be viable on datafusion, not delegated to lucene.
    JSONObject result =
        executeWithProfile(
            "source = " + INDEX + " | mvexpand tags | stats count() by tags", "/_plugins/_ppl");
    JSONArray fullPlan =
        result.getJSONObject("profile").getJSONObject("plan").getJSONArray("full_plan");
    boolean sawDatafusion = false;
    boolean sawLuceneOnly = false;
    for (int i = 0; i < fullPlan.length(); i++) {
      String line = fullPlan.getString(i);
      if (line.contains("viableBackends=[[datafusion]]")) sawDatafusion = true;
      if (line.contains("viableBackends=[[lucene]]")) sawLuceneOnly = true;
    }
    assertTrue("multi-value group-by executes on the analytics engine (datafusion)", sawDatafusion);
    assertFalse("multi-value group-by is not delegated wholesale to lucene", sawLuceneOnly);
  }

  // ==================== cross-shard LIST projection (#23040) ====================

  @Test
  public void testMultiShardListProjection() throws IOException {
    ensureSetup();
    // Projecting the stored LIST column across shards exercises the Utf8/Utf8View LIST-child schema
    // boundary. Each doc keeps its array intact; assert the full (id, tags) set
    // order-independently.
    JSONObject r = query("source = " + INDEX + " | fields id, tags | sort id");
    List<String> expected =
        List.of("d1,prod", "d2,blue", "d3,prod|blue", "d4,green|prod", "d5,blue|green", "d6,prod");
    JSONArray rows = r.getJSONArray("datarows");
    List<String> actual = new ArrayList<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      String id = row.getString(0);
      Object tagsObj = row.get(1);
      List<String> tags = new ArrayList<>();
      if (tagsObj instanceof JSONArray arr) {
        for (int j = 0; j < arr.length(); j++) tags.add(arr.getString(j));
      } else {
        tags.add(String.valueOf(tagsObj));
      }
      actual.add(id + "," + String.join("|", tags));
    }
    actual.sort(String::compareTo);
    List<String> exp = new ArrayList<>(expected);
    exp.sort(String::compareTo);
    assertEquals(exp, actual);
  }

  // ==================== helpers ====================

  /** Runs a PPL query and returns the parsed response. */
  private JSONObject query(String ppl) throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(String.format(Locale.ROOT, "{\"query\": \"%s\"}", ppl));
    return new JSONObject(entityAsString(client().performRequest(request)));
  }

  private JSONObject executeWithProfile(String ppl, String endpoint) throws IOException {
    Request request = new Request("POST", endpoint);
    request.setJsonEntity(
        String.format(Locale.ROOT, "{\"query\": \"%s\", \"profile\": true}", ppl));
    return new JSONObject(entityAsString(client().performRequest(request)));
  }

  /**
   * Asserts the datarows equal {@code expected} (each entry "col0,col1") regardless of row order.
   */
  private void assertRowsUnordered(JSONObject result, List<String> expected, String... cols)
      throws IOException {
    JSONArray rows = result.getJSONArray("datarows");
    List<String> actual = new ArrayList<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      List<String> parts = new ArrayList<>();
      for (int c = 0; c < row.length(); c++) parts.add(String.valueOf(row.get(c)));
      actual.add(String.join(",", parts));
    }
    actual.sort(String::compareTo);
    List<String> exp = new ArrayList<>(expected);
    exp.sort(String::compareTo);
    assertEquals(exp, actual);
  }

  private static String entityAsString(Response response) throws IOException {
    return new String(
        response.getEntity().getContent().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
  }
}
