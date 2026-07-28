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

  // === B. Operator tree — all pushed down ===

  @Test
  public void operatorTreeAllPushedDown() throws IOException {
    JSONObject result =
        executeAnalyze(
            "source=" + TEST_INDEX_ACCOUNT + " | where age > 30 | fields firstname, age");
    JSONArray tree = result.getJSONArray("operator_tree");

    // Single physical node → all segments merged into one entry
    assertEquals(1, tree.length());
    JSONObject node = tree.getJSONObject(0);
    assertTrue(node.getBoolean("is_pushed_down"));

    JSONArray nodeTypes = node.getJSONArray("node_type");
    assertTrue(nodeTypes.toString().contains("SearchFrom"));
    assertTrue(nodeTypes.toString().contains("WhereCommand"));
    assertTrue(nodeTypes.toString().contains("FieldsCommand"));
  }

  @Test
  public void operatorTreeAllPushedDownWithStats() throws IOException {
    JSONObject result =
        executeAnalyze(
            "source=" + TEST_INDEX_ACCOUNT + " | where age > 30 | stats count() by gender");
    JSONArray tree = result.getJSONArray("operator_tree");

    assertEquals(1, tree.length());
    JSONObject node = tree.getJSONObject(0);
    assertTrue(node.getBoolean("is_pushed_down"));

    JSONArray nodeTypes = node.getJSONArray("node_type");
    assertTrue(nodeTypes.toString().contains("SearchFrom"));
    assertTrue(nodeTypes.toString().contains("WhereCommand"));
    assertTrue(nodeTypes.toString().contains("StatsCommand"));
  }

  // === C. Operator tree — partial pushdown ===

  @Test
  public void operatorTreePartialPushdown() throws IOException {
    JSONObject result =
        executeAnalyze(
            "source="
                + TEST_INDEX_ACCOUNT
                + " | where age > 30 | eval name = firstname | fields name, age");
    JSONArray tree = result.getJSONArray("operator_tree");

    // At least 2 entries: pushed-down group + non-pushed group
    assertTrue(tree.length() >= 2);
    // First entry should be pushed down
    assertTrue(tree.getJSONObject(0).optBoolean("is_pushed_down", false));
    // Last entry should NOT be pushed down
    assertFalse(tree.getJSONObject(tree.length() - 1).optBoolean("is_pushed_down", false));
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

  // === E. Timing correctness ===

  @Test
  public void operatorTreeHasTimings() throws IOException {
    JSONObject result =
        executeAnalyze("source=" + TEST_INDEX_ACCOUNT + " | where age > 30 | fields firstname");
    JSONArray tree = result.getJSONArray("operator_tree");

    for (int i = 0; i < tree.length(); i++) {
      JSONObject node = tree.getJSONObject(i);
      assertTrue("node " + i + " has actual_time_ms", node.has("actual_time_ms"));
      assertTrue("node " + i + " has actual_rows", node.has("actual_rows"));
      assertTrue(node.getLong("actual_rows") >= 0);
    }
  }

  @Test
  public void operatorTreeTimingsSumApproximatesPlanRoot() throws IOException {
    JSONObject result =
        executeAnalyze(
            "source="
                + TEST_INDEX_ACCOUNT
                + " | where age > 30 | eval x = age * 2 | fields x, firstname");
    JSONArray tree = result.getJSONArray("operator_tree");
    JSONObject profile = result.getJSONObject("profile");

    double totalOperatorTime = 0;
    for (int i = 0; i < tree.length(); i++) {
      String timeStr = tree.getJSONObject(i).getString("actual_time_ms");
      totalOperatorTime += Double.parseDouble(timeStr.replace(" ms", ""));
    }
    double planRootTime = profile.getJSONObject("plan").getDouble("time_ms");

    // Exclusive times should sum to roughly the root inclusive time.
    // Allow generous tolerance for off-spine subtree time not captured.
    assertTrue(
        "operator times (" + totalOperatorTime + ") roughly match plan root (" + planRootTime + ")",
        totalOperatorTime <= planRootTime * 2.0 && totalOperatorTime >= planRootTime * 0.1);
  }

  // === F. Estimated rows ===

  @Test
  public void operatorTreeHasEstimatedRows() throws IOException {
    JSONObject result =
        executeAnalyze("source=" + TEST_INDEX_ACCOUNT + " | where age > 30 | fields firstname");
    JSONArray tree = result.getJSONArray("operator_tree");

    for (int i = 0; i < tree.length(); i++) {
      JSONObject node = tree.getJSONObject(i);
      assertTrue("node " + i + " has estimated_rows", node.has("estimated_rows"));
      assertTrue(node.getLong("estimated_rows") > 0);
    }
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
    // Profile and operator tree should still be present
    assertTrue(result.has("profile"));
    assertTrue(result.has("operator_tree"));
    assertTrue(result.getJSONArray("operator_tree").length() > 0);
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

  // === K. Pushdown disabled ===

  @Test
  public void analyzeWithPushdownDisabledShowsNoPushdown() throws IOException {
    // Disable pushdown
    updateClusterSettings(
        new ClusterSetting(
            "transient",
            org.opensearch.sql.common.setting.Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(),
            "false"));
    try {
      JSONObject result =
          executeAnalyze(
              "source=" + TEST_INDEX_ACCOUNT + " | where age > 30 | fields firstname, age");
      JSONArray tree = result.getJSONArray("operator_tree");

      // With pushdown disabled, nothing should be marked as pushed down
      // (or it should have multiple nodes since operations stay separate)
      if (tree.length() == 1) {
        // If still 1 node, it shouldn't be marked pushed_down
        assertFalse(tree.getJSONObject(0).optBoolean("is_pushed_down", false));
      } else {
        // Multiple nodes means operations weren't merged
        assertTrue(tree.length() > 1);
      }
    } finally {
      // Re-enable pushdown
      updateClusterSettings(
          new ClusterSetting(
              "transient",
              org.opensearch.sql.common.setting.Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(),
              "true"));
    }
  }
}
