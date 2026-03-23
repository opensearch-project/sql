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

  @Test
  public void testExplainBasicQuery() throws IOException {
    String explainResult =
        explainQueryToString("source = opensearch.parquet_logs | fields ts, message");
    assertTrue("Explain should contain LogicalProject", explainResult.contains("LogicalProject"));
    assertTrue(
        "Explain should contain LogicalTableScan", explainResult.contains("LogicalTableScan"));
    assertTrue(
        "Explain should reference parquet_logs table", explainResult.contains("parquet_logs"));
  }

  @Test
  public void testExplainFilterQuery() throws IOException {
    String explainResult =
        explainQueryToString(
            "source = opensearch.parquet_logs | where status = 200 | fields ts, message");
    assertTrue("Explain should contain LogicalFilter", explainResult.contains("LogicalFilter"));
    assertTrue("Explain should contain LogicalProject", explainResult.contains("LogicalProject"));
  }

  @Test
  public void testExplainAggregation() throws IOException {
    String explainResult =
        explainQueryToString("source = opensearch.parquet_logs | stats count() by status");
    assertTrue(
        "Explain should contain LogicalAggregate", explainResult.contains("LogicalAggregate"));
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
