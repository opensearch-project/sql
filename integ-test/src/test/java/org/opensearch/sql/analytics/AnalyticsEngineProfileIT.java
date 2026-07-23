/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analytics;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.test.rest.OpenSearchRestTestCase;

/**
 * Integration tests for the analytics engine profile API (profile=true). Verifies that PPL and SQL
 * queries with profile=true return stage profiling, and that /_explain returns only the logical
 * plan without execution.
 *
 * <p>Runs against the analyticsEngineProfileIT cluster which has the full analytics engine stack.
 */
public class AnalyticsEngineProfileIT extends OpenSearchRestTestCase {

  private static final String INDEX = "profile_test";
  private static boolean initialized = false;

  private void ensureSetup() throws IOException {
    if (initialized) return;
    enableCalcite();
    createCompositeIndex();
    ingestData();
    initialized = true;
  }

  private void enableCalcite() throws IOException {
    Request req = new Request("PUT", "/_cluster/settings");
    req.setJsonEntity("{\"persistent\":{\"plugins.calcite.enabled\":true}}");
    client().performRequest(req);
  }

  private void createCompositeIndex() throws IOException {
    try {
      Request req = new Request("PUT", "/" + INDEX);
      req.setJsonEntity(
          """
          {
            "settings": {
              "number_of_shards": 2,
              "number_of_replicas": 0,
              "index.pluggable.dataformat.enabled": true,
              "index.pluggable.dataformat": "composite"
            },
            "mappings": {
              "properties": {
                "name": {"type": "keyword"},
                "score": {"type": "double"}
              }
            }
          }
          """);
      client().performRequest(req);
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() != 400) throw e;
      // Index already exists
    }
  }

  private void ingestData() throws IOException {
    Request bulk = new Request("POST", "/_bulk");
    bulk.addParameter("refresh", "true");
    bulk.setJsonEntity(
        String.format(
            Locale.ROOT,
            """
            {"index":{"_index":"%s"}}
            {"name":"alice","score":95.5}
            {"index":{"_index":"%s"}}
            {"name":"bob","score":87.3}
            {"index":{"_index":"%s"}}
            {"name":"carol","score":91.0}
            """,
            INDEX,
            INDEX,
            INDEX));
    RequestOptions.Builder opts = RequestOptions.DEFAULT.toBuilder();
    opts.addHeader("Content-Type", "application/x-ndjson");
    bulk.setOptions(opts);
    client().performRequest(bulk);
  }

  @Test
  public void testPplProfileReturnsStages() throws IOException {
    ensureSetup();
    JSONObject result =
        executeWithProfile("source = " + INDEX + " | stats avg(score) by name", "/_plugins/_ppl");

    assertTrue("has schema", result.has("schema"));
    assertTrue("has datarows", result.has("datarows"));
    assertTrue("has profile", result.has("profile"));

    JSONObject profile = result.getJSONObject("profile");
    assertTrue("has summary", profile.has("summary"));
    assertTrue("summary has total_time_ms", profile.getJSONObject("summary").has("total_time_ms"));
    assertTrue("has phases", profile.has("phases"));
    JSONObject phases = profile.getJSONObject("phases");
    assertTrue("phase analyze", phases.getJSONObject("analyze").has("time_ms"));
    assertTrue("phase execute", phases.getJSONObject("execute").has("time_ms"));
    assertTrue("phase format", phases.getJSONObject("format").has("time_ms"));

    JSONObject plan = profile.getJSONObject("plan");
    assertTrue("has query_id", plan.has("query_id"));
    assertTrue("has planning_time_ms", plan.has("planning_time_ms"));
    assertTrue("has execution_time_ms", plan.has("execution_time_ms"));
    assertTrue("has full_plan", plan.has("full_plan"));

    JSONArray stages = plan.getJSONArray("stages");
    assertTrue("at least one stage", stages.length() >= 1);

    JSONObject stage = stages.getJSONObject(0);
    assertTrue("stage has stage_id", stage.has("stage_id"));
    assertTrue("stage has execution_type", stage.has("execution_type"));
    assertTrue("stage has state", stage.has("state"));
    assertTrue("stage has elapsed_ms", stage.has("elapsed_ms"));
    assertTrue("stage has tasks", stage.has("tasks"));
  }

  @Test
  public void testSqlProfileReturnsStages() throws IOException {
    ensureSetup();
    JSONObject result = executeWithProfile("SELECT * FROM " + INDEX, "/_plugins/_sql");

    assertTrue("has schema", result.has("schema"));
    assertTrue("has datarows", result.has("datarows"));
    assertTrue("has profile", result.has("profile"));

    JSONObject profile = result.getJSONObject("profile");
    assertTrue("has summary", profile.has("summary"));
    assertTrue("has phases", profile.has("phases"));
    JSONObject plan = profile.getJSONObject("plan");
    assertTrue("has query_id", plan.has("query_id"));
    assertTrue("has stages", plan.has("stages"));
    JSONArray stages = plan.getJSONArray("stages");
    assertTrue("at least one stage", stages.length() >= 1);
  }

  @Test
  public void testPplProfileStagesShowSucceeded() throws IOException {
    ensureSetup();
    JSONObject result =
        executeWithProfile("source = " + INDEX + " | fields name, score", "/_plugins/_ppl");

    JSONObject profile = result.getJSONObject("profile");
    JSONArray stages = profile.getJSONObject("plan").getJSONArray("stages");

    for (int i = 0; i < stages.length(); i++) {
      JSONObject stage = stages.getJSONObject(i);
      assertEquals("stage succeeded", "SUCCEEDED", stage.getString("state"));
      assertTrue("elapsed_ms non-negative", stage.getLong("elapsed_ms") >= 0);
    }
  }

  @Test
  public void testPplProfileTasksHaveNodeAndTiming() throws IOException {
    ensureSetup();
    JSONObject result =
        executeWithProfile("source = " + INDEX + " | fields name", "/_plugins/_ppl");

    JSONObject profile = result.getJSONObject("profile");
    JSONArray stages = profile.getJSONObject("plan").getJSONArray("stages");

    boolean foundTasks = false;
    for (int i = 0; i < stages.length(); i++) {
      JSONArray tasks = stages.getJSONObject(i).getJSONArray("tasks");
      if (tasks.length() > 0) {
        foundTasks = true;
        JSONObject task = tasks.getJSONObject(0);
        assertTrue("task has node", task.has("node"));
        assertTrue("task has state", task.has("state"));
        assertTrue("task has elapsed_ms", task.has("elapsed_ms"));
      }
    }
    assertTrue("at least one stage has tasks", foundTasks);
  }

  @Test
  public void testStaticSearchStillRunsThroughAnalyticsEngine() throws IOException {
    ensureSetup();
    JSONObject result =
        executeWithProfile(
            "search source=" + INDEX + " name=alice | fields name", "/_plugins/_ppl");

    assertEquals("alice", result.getJSONArray("datarows").getJSONArray(0).getString(0));
  }

  @Test
  public void testPplExplainReturnsOnlyPlan() throws IOException {
    ensureSetup();
    Request request = new Request("POST", "/_plugins/_ppl/_explain");
    request.setJsonEntity(
        String.format(Locale.ROOT, "{\"query\": \"source = %s | fields name, score\"}", INDEX));
    Response response = client().performRequest(request);
    JSONObject result = new JSONObject(entityAsString(response));

    assertTrue("has calcite", result.has("calcite"));
    JSONObject calcite = result.getJSONObject("calcite");
    assertTrue("has logical", calcite.has("logical"));
    assertFalse("no profile in explain", calcite.has("profile"));
  }

  @Test
  public void testImplicitFormatExplainShowsCorrelatedPlan() throws IOException {
    ensureSetup();
    Request request = new Request("POST", "/_plugins/_ppl/_explain");
    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{\"query\": \"search source=%s [ search source=%s name=alice | fields name | head"
                + " 1 ] | fields name\"}",
            INDEX,
            INDEX));
    Response response = client().performRequest(request);
    JSONObject result = new JSONObject(entityAsString(response));

    String logical = result.getJSONObject("calcite").get("logical").toString();
    assertTrue(logical, logical.contains("LogicalCorrelate"));
    assertTrue(logical, logical.contains("dynamicQueryString=$cor"));
    assertFalse(logical, logical.contains("SCALAR_QUERY"));
  }

  @Test
  public void testSqlExplainReturnsOnlyPlan() throws IOException {
    ensureSetup();
    Request request = new Request("POST", "/_plugins/_sql/_explain");
    request.setJsonEntity(String.format(Locale.ROOT, "{\"query\": \"SELECT * FROM %s\"}", INDEX));
    Response response = client().performRequest(request);
    JSONObject result = new JSONObject(entityAsString(response));

    assertTrue("has calcite", result.has("calcite"));
    JSONObject calcite = result.getJSONObject("calcite");
    assertTrue("has logical", calcite.has("logical"));
    assertFalse("no profile in explain", calcite.has("profile"));
  }

  private JSONObject executeWithProfile(String query, String endpoint) throws IOException {
    Request request = new Request("POST", endpoint);
    request.setJsonEntity(
        String.format(Locale.ROOT, "{\"query\": \"%s\", \"profile\": true}", query));
    Response response = client().performRequest(request);
    return new JSONObject(entityAsString(response));
  }

  private static String entityAsString(Response response) throws IOException {
    return new String(
        response.getEntity().getContent().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
  }
}
