/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.ResponseException;

/**
 * Integration tests for PPL queries routed through the analytics engine path (Project Mustang).
 * Queries targeting "parquet_*" indices are routed to {@code RestUnifiedQueryAction} which uses
 * {@code AnalyticsExecutionEngine} with a stub {@code QueryPlanExecutor}.
 *
 * <p>These tests validate the full pipeline: REST request → routing → planning via
 * UnifiedQueryPlanner → execution via AnalyticsExecutionEngine → response formatting.
 *
 * <p>The stub executor always returns the full table rows regardless of the logical plan. After
 * projection (| fields), the execution engine maps row values by position — so projected columns
 * get the values from the corresponding positions in the full row, not the actual projected column.
 * This is expected behavior for a stub; the real analytics engine will evaluate the plan correctly.
 */
public class AnalyticsPPLIT extends PPLIntegTestCase {

  @Override
  protected void init() throws Exception {
    // No index loading needed — stub schema and data are hardcoded
    // in RestUnifiedQueryAction and StubQueryPlanExecutor
  }

  // --- Full table scan tests with schema + data verification ---

  @Test
  public void testBasicQuerySchemaAndData() throws IOException {
    JSONObject result = executeQuery("source = opensearch.parquet_logs");
    verifySchema(
        result,
        schema("ts", "timestamp"),
        schema("status", "integer"),
        schema("message", "keyword"),
        schema("ip_addr", "keyword"));
    verifyNumOfRows(result, 3);
    // Verify actual row data from StubQueryPlanExecutor (timestamps in UTC)
    verifyDataRows(
        result,
        rows("2024-01-15 10:30:00", 200, "Request completed", "192.168.1.1"),
        rows("2024-01-15 10:31:00", 200, "Health check OK", "192.168.1.2"),
        rows("2024-01-15 10:32:00", 500, "Internal server error", "192.168.1.3"));
  }

  @Test
  public void testParquetMetricsSchemaAndData() throws IOException {
    JSONObject result = executeQuery("source = opensearch.parquet_metrics");
    verifySchema(
        result,
        schema("ts", "timestamp"),
        schema("cpu", "double"),
        schema("memory", "double"),
        schema("host", "keyword"));
    verifyNumOfRows(result, 2);
    verifyDataRows(
        result,
        rows("2024-01-15 10:30:00", 75.5, 8192.5, "host-1"),
        rows("2024-01-15 10:31:00", 82.3, 7680.5, "host-2"));
  }

  // --- Response format validation ---

  @Test
  public void testResponseFormatHasRequiredFields() throws IOException {
    JSONObject result = executeQuery("source = opensearch.parquet_logs");
    assertTrue("Response should have 'schema' field", result.has("schema"));
    assertTrue("Response should have 'datarows' field", result.has("datarows"));
    assertTrue("Response should have 'total' field", result.has("total"));
    assertTrue("Response should have 'size' field", result.has("size"));
    assertTrue("Response should have 'status' field", result.has("status"));
    assertEquals(200, result.getInt("status"));
  }

  @Test
  public void testTotalAndSizeMatchRowCount() throws IOException {
    JSONObject result = executeQuery("source = opensearch.parquet_logs");
    int rowCount = result.getJSONArray("datarows").length();
    assertEquals(rowCount, result.getInt("total"));
    assertEquals(rowCount, result.getInt("size"));
  }

  // --- Projection tests (schema verification only — stub doesn't evaluate projections) ---

  @Test
  public void testFieldsProjectionChangesSchema() throws IOException {
    JSONObject result = executeQuery("source = opensearch.parquet_logs | fields ts, message");
    verifySchema(result, schema("ts", "timestamp"), schema("message", "keyword"));
    verifyNumOfRows(result, 3);
  }

  @Test
  public void testSingleFieldProjection() throws IOException {
    JSONObject result = executeQuery("source = opensearch.parquet_logs | fields status");
    verifySchema(result, schema("status", "integer"));
    verifyNumOfRows(result, 3);
  }

  // --- Explain tests ---
  // Explain returns JSON: { "calcite": { "logical": "...", "physical": null, "extended": null } }
  // We parse the full structure and verify the logical plan content.

  @Test
  public void testExplainResponseStructure() throws IOException {
    String raw = explainQueryToString("source = opensearch.parquet_logs | fields ts, message");
    JSONObject response = new JSONObject(raw);

    // Response must have "calcite" object
    assertTrue("Explain should have 'calcite' key", response.has("calcite"));
    JSONObject calcite = response.getJSONObject("calcite");

    // "logical" must be present and non-empty
    assertTrue("Explain should have 'logical' key", calcite.has("logical"));
    String logical = calcite.getString("logical");
    assertFalse("Logical plan should not be empty", logical.isEmpty());

    // "physical" and "extended" should be null (analytics engine not available)
    assertTrue("Physical plan should be null", calcite.isNull("physical"));
    assertTrue("Extended plan should be null", calcite.isNull("extended"));
  }

  @Test
  public void testExplainProjectAndScan() throws IOException {
    String raw = explainQueryToString("source = opensearch.parquet_logs | fields ts, message");
    String logical = extractLogicalPlan(raw);

    // Verify the plan contains the expected Calcite operators
    assertTrue("Plan should contain LogicalProject", logical.contains("LogicalProject"));
    assertTrue("Plan should contain LogicalTableScan", logical.contains("LogicalTableScan"));
    assertTrue("Plan should reference parquet_logs", logical.contains("parquet_logs"));
    // Verify projected columns appear in the plan
    assertTrue("Plan should reference ts column", logical.contains("ts"));
    assertTrue("Plan should reference message column", logical.contains("message"));
  }

  @Test
  public void testExplainFilterPlan() throws IOException {
    String raw =
        explainQueryToString(
            "source = opensearch.parquet_logs | where status = 200 | fields ts, message");
    String logical = extractLogicalPlan(raw);

    assertTrue("Plan should contain LogicalProject", logical.contains("LogicalProject"));
    assertTrue("Plan should contain LogicalFilter", logical.contains("LogicalFilter"));
    assertTrue("Plan should contain LogicalTableScan", logical.contains("LogicalTableScan"));
    // Verify filter condition appears
    assertTrue("Plan should contain filter condition with 200", logical.contains("200"));
  }

  @Test
  public void testExplainAggregationPlan() throws IOException {
    String raw = explainQueryToString("source = opensearch.parquet_logs | stats count() by status");
    String logical = extractLogicalPlan(raw);

    assertTrue("Plan should contain LogicalAggregate", logical.contains("LogicalAggregate"));
    assertTrue("Plan should contain COUNT()", logical.contains("COUNT()"));
  }

  @Test
  public void testExplainSortPlan() throws IOException {
    String raw = explainQueryToString("source = opensearch.parquet_logs | sort ts");
    String logical = extractLogicalPlan(raw);

    assertTrue("Plan should contain LogicalSort", logical.contains("LogicalSort"));
  }

  /** Extract the logical plan string from the explain JSON response. */
  private String extractLogicalPlan(String explainResponse) {
    JSONObject response = new JSONObject(explainResponse);
    return response.getJSONObject("calcite").getString("logical");
  }

  // --- Error handling tests ---

  @Test
  public void testSyntaxErrorReturnsClientError() {
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () -> executeQuery("source = opensearch.parquet_logs | invalid_command"));
    int statusCode = e.getResponse().getStatusLine().getStatusCode();
    assertTrue(
        "Syntax error should return 4xx, got " + statusCode, statusCode >= 400 && statusCode < 500);
  }

  // --- Regression tests ---

  @Test
  public void testNonParquetQueryStillWorks() throws IOException {
    loadIndex(Index.ACCOUNT);
    JSONObject result =
        executeQuery(String.format("source=%s | head 1 | fields firstname", TEST_INDEX_ACCOUNT));
    assertNotNull("Non-parquet query should return results", result);
    assertTrue("Non-parquet query should have datarows", result.has("datarows"));
    assertTrue(
        "Non-parquet query should return data", result.getJSONArray("datarows").length() > 0);
  }

  @Test
  public void testNonParquetAggregationStillWorks() throws IOException {
    loadIndex(Index.ACCOUNT);
    JSONObject result =
        executeQuery(String.format("source=%s | stats count()", TEST_INDEX_ACCOUNT));
    assertTrue("Non-parquet aggregation should work", result.getInt("total") > 0);
  }
}
