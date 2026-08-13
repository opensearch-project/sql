/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.QUERY_API_ENDPOINT;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteAnalyzeIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
  }

  // === Helper ===

  private JSONObject executeAnalyze(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(
        String.format(Locale.ROOT, "{\"query\": \"%s\", \"analyze\": true}", query));
    RequestOptions.Builder opts = RequestOptions.DEFAULT.toBuilder();
    opts.addHeader("Content-Type", "application/json");
    request.setOptions(opts);
    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response, true));
  }

  private JSONObject executeProfile(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(
        String.format(Locale.ROOT, "{\"query\": \"%s\", \"profile\": true}", query));
    RequestOptions.Builder opts = RequestOptions.DEFAULT.toBuilder();
    opts.addHeader("Content-Type", "application/json");
    request.setOptions(opts);
    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response, true));
  }

  // === A. Query result correctness ===

  @Test
  public void analyzeResultsMatchNormalExecution() throws IOException {
    String query = "source=" + TEST_INDEX_ACCOUNT + " | where age > 30 | fields firstname, age";
    JSONObject normal = executeQuery(query);
    JSONObject analyzed = executeAnalyze(query);

    // Schema should match
    assertEquals(normal.getJSONArray("schema").length(), analyzed.getJSONArray("schema").length());
    // Row counts should match
    assertEquals(normal.getInt("total"), analyzed.getInt("total"));
    assertEquals(normal.getInt("size"), analyzed.getInt("size"));
    // Datarows should have same length
    assertEquals(
        normal.getJSONArray("datarows").length(), analyzed.getJSONArray("datarows").length());
  }

  @Test
  public void analyzeResultsMatchWithAggregation() throws IOException {
    String query = "source=" + TEST_INDEX_ACCOUNT + " | stats count() by gender";
    JSONObject normal = executeQuery(query);
    JSONObject analyzed = executeAnalyze(query);

    assertEquals(normal.getInt("total"), analyzed.getInt("total"));
    assertEquals(
        normal.getJSONArray("datarows").length(), analyzed.getJSONArray("datarows").length());
  }

  // === D. Profile structure ===

  @Test
  public void analyzeIncludesProfileWithAllPhases() throws IOException {
    JSONObject result =
        executeAnalyze("source=" + TEST_INDEX_ACCOUNT + " | where age > 30 | fields firstname");
    assertTrue(result.has("profile"));

    JSONObject profile = result.getJSONObject("profile");
    assertTrue(profile.has("summary"));
    assertTrue(profile.has("phases"));
    assertTrue(profile.has("plan"));

    JSONObject phases = profile.getJSONObject("phases");
    assertTrue(phases.has("analyze"));
    assertTrue(phases.has("optimize"));
    assertTrue(phases.has("execute"));
    assertTrue(phases.has("format"));

    // All phase times should be non-negative
    assertTrue(phases.getJSONObject("analyze").getDouble("time_ms") >= 0);
    assertTrue(phases.getJSONObject("optimize").getDouble("time_ms") >= 0);
    assertTrue(phases.getJSONObject("execute").getDouble("time_ms") >= 0);
    assertTrue(phases.getJSONObject("format").getDouble("time_ms") >= 0);
  }

  @Test
  public void analyzeProfilePlanHasNodeInfo() throws IOException {
    JSONObject result =
        executeAnalyze("source=" + TEST_INDEX_ACCOUNT + " | where age > 30 | fields firstname");
    JSONObject plan = result.getJSONObject("profile").getJSONObject("plan");

    assertTrue(plan.has("node"));
    assertTrue(plan.has("time_ms"));
    assertTrue(plan.has("rows"));
    assertTrue(plan.getDouble("time_ms") >= 0);
    assertTrue(plan.getLong("rows") >= 0);
  }

  // === G. Logical and physical plan presence ===

  @Test
  public void analyzeIncludesLogicalAndPhysicalPlan() throws IOException {
    JSONObject result =
        executeAnalyze("source=" + TEST_INDEX_ACCOUNT + " | where age > 30 | fields firstname");

    assertTrue(result.has("logicalPlan"));
    assertTrue(result.has("physicalPlan"));

    JSONArray logicalPlan = result.getJSONArray("logicalPlan");
    JSONArray physicalPlan = result.getJSONArray("physicalPlan");

    assertTrue(logicalPlan.length() > 0);
    assertTrue(physicalPlan.length() > 0);

    // Logical plan should contain known node types
    String logicalStr = logicalPlan.toString();
    assertTrue(logicalStr.contains("LogicalFilter") || logicalStr.contains("LogicalProject"));
  }

  // === H. Degenerate cases ===

  @Test
  public void analyzeEmptyResults() throws IOException {
    JSONObject result =
        executeAnalyze("source=" + TEST_INDEX_ACCOUNT + " | where age > 99999 | fields firstname");

    assertEquals(0, result.getInt("total"));
    assertEquals(0, result.getJSONArray("datarows").length());
    // Profile should still be present
    assertTrue(result.has("profile"));
  }

  @Test
  public void analyzeNonexistentIndexReturnsError() {
    assertThrows(
        ResponseException.class, () -> executeAnalyze("source=nonexistent_index_xyz | fields a"));
  }

  @Test
  public void analyzeSyntaxErrorReturnsError() {
    assertThrows(ResponseException.class, () -> executeAnalyze("this is not valid ppl"));
  }

  @Test
  public void analyzeLongArithmeticOverflowReturnsError() throws IOException {
    // Regression guard: the analyze path must apply checked arithmetic just like execute, so a
    // BIGINT overflow surfaces as a client error instead of silently wrapping. See issue #5164.
    // 9223372036854775807 is Long.MAX_VALUE; + 1 overflows and has no wider integer type.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeAnalyze(
                    "source="
                        + TEST_INDEX_ACCOUNT
                        + " | head 1 | eval overflow = 9223372036854775807 + 1 | fields overflow"));
    assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
    String body = getResponseBody(e.getResponse(), true);
    assertTrue("expected an overflow error, got: " + body, body.contains("overflow"));
  }

  // === I. Schema correctness ===

  @Test
  public void analyzeSchemaMatchesQueryFields() throws IOException {
    JSONObject result =
        executeAnalyze(
            "source=" + TEST_INDEX_ACCOUNT + " | where age > 30 | fields firstname, age");
    JSONArray schema = result.getJSONArray("schema");

    assertEquals(2, schema.length());
    assertEquals("firstname", schema.getJSONObject(0).getString("name"));
    assertEquals("age", schema.getJSONObject(1).getString("name"));
  }

  // === L. Recommendations ===

  @Test
  public void analyzeIncludesRecommendationsArray() throws IOException {
    JSONObject result =
        executeAnalyze("source=" + TEST_INDEX_ACCOUNT + " | where age > 30 | fields firstname");
    // recommendations is always present (possibly empty) and is an array.
    assertTrue(result.has("recommendations"));
    result.getJSONArray("recommendations");
  }

  @Test
  public void analyzeRecommendationsHaveWellFormedShape() throws IOException {
    // A non-selective filter (age >= 0 keeps every row) should trip the "Ineffective Filter" rule;
    // even if it does not on this data, any emitted recommendation must have the required fields.
    JSONObject result =
        executeAnalyze("source=" + TEST_INDEX_ACCOUNT + " | where age >= 0 | fields firstname");
    JSONArray recs = result.getJSONArray("recommendations");
    for (int i = 0; i < recs.length(); i++) {
      JSONObject rec = recs.getJSONObject(i);
      assertTrue("recommendation has severity", rec.has("severity"));
      assertTrue("recommendation has rule", rec.has("rule"));
      assertTrue("recommendation has message", rec.has("message"));
      String severity = rec.getString("severity");
      assertTrue(
          "severity is a known level",
          severity.equals("INFO") || severity.equals("WARNING") || severity.equals("CRITICAL"));
    }
  }

  @Test
  public void analyzeRecommendationRulesAreKnownAndConsistent() throws IOException {
    // Whatever rules fire against a real plan/profile, each must be one of the known rule names
    // and carry the fields that rule populates. This validates the rules run end-to-end against a
    // real QueryProfile.PlanNode tree (node names, timings) rather than only synthetic unit input.
    JSONObject result =
        executeAnalyze(
            "source=" + TEST_INDEX_ACCOUNT + " | where age >= 0 | stats count() by state");
    JSONArray recs = result.getJSONArray("recommendations");
    for (int i = 0; i < recs.length(); i++) {
      JSONObject rec = recs.getJSONObject(i);
      String rule = rec.getString("rule");
      assertTrue(
          "unexpected rule: " + rule,
          rule.equals("Ineffective Filter")
              || rule.equals("Join Row Explosion")
              || rule.equals("Expensive Sort")
              || rule.equals("Bottleneck Stage")
              || rule.equals("Optimize Phase Dominates"));
      // affected_node accompanies the per-node rules.
      if (rule.equals("Ineffective Filter")
          || rule.equals("Join Row Explosion")
          || rule.equals("Expensive Sort")
          || rule.equals("Bottleneck Stage")) {
        assertTrue("per-node rule should name a node", rec.has("affected_node"));
      }
    }
  }

  // === J. Profile timing similarity to standalone profile endpoint ===

  @Test
  public void analyzeTimingsInSameOrderOfMagnitudeAsProfile() throws IOException {
    String query = "source=" + TEST_INDEX_ACCOUNT + " | where age > 30 | stats count() by gender";

    JSONObject profileResult = executeProfile(query);
    JSONObject analyzeResult = executeAnalyze(query);

    double profileTotal =
        profileResult.getJSONObject("profile").getJSONObject("summary").getDouble("total_time_ms");
    double analyzeTotal =
        analyzeResult.getJSONObject("profile").getJSONObject("summary").getDouble("total_time_ms");

    // Should be within 5x of each other (generous for CI environments)
    assertTrue(
        "analyze total (" + analyzeTotal + ") within 5x of profile total (" + profileTotal + ")",
        analyzeTotal < profileTotal * 5 && analyzeTotal > profileTotal / 5);
  }
}
