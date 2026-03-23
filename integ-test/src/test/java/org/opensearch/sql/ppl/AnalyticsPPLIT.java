/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

/**
 * Integration tests for PPL queries routed through the analytics engine path (Project Mustang).
 * Queries targeting "parquet_*" indices are routed to {@code RestUnifiedQueryAction} which uses
 * {@code AnalyticsExecutionEngine} with a stub {@code QueryPlanExecutor}.
 *
 * <p>These tests validate the full pipeline: REST request → routing → planning via
 * UnifiedQueryPlanner → execution via AnalyticsExecutionEngine → response formatting.
 *
 * <p>Note: The stub executor ignores the logical plan and always returns all rows for the matched
 * table. Tests that use projection (| fields) or filter (| where) will still get all rows back with
 * the projected schema, but the data values may not match the projected columns since the stub does
 * not actually evaluate the plan.
 */
public class AnalyticsPPLIT extends PPLIntegTestCase {

  @Override
  protected void init() throws Exception {
    // No index loading needed — stub schema and data are hardcoded
    // in RestUnifiedQueryAction and StubQueryPlanExecutor
  }

  @Test
  public void testBasicQueryReturnsResults() throws IOException {
    JSONObject result = executeQuery("source = opensearch.parquet_logs");
    verifySchema(
        result,
        schema("ts", "timestamp"),
        schema("status", "integer"),
        schema("message", "keyword"),
        schema("ip_addr", "keyword"));
    verifyNumOfRows(result, 3);
  }

  @Test
  public void testParquetMetricsTable() throws IOException {
    JSONObject result = executeQuery("source = opensearch.parquet_metrics");
    verifySchema(
        result,
        schema("ts", "timestamp"),
        schema("cpu", "double"),
        schema("memory", "double"),
        schema("host", "keyword"));
    verifyNumOfRows(result, 2);
  }

  @Test
  public void testResponseFormatHasRequiredFields() throws IOException {
    JSONObject result = executeQuery("source = opensearch.parquet_logs");
    assertTrue(result.has("schema"));
    assertTrue(result.has("datarows"));
    assertTrue(result.has("total"));
    assertTrue(result.has("size"));
    assertTrue(result.has("status"));
    assertEquals(200, result.getInt("status"));
  }

  @Test
  public void testExplainQuery() throws IOException {
    String explainResult =
        explainQueryToString("source = opensearch.parquet_logs | fields ts, message");
    assertTrue(explainResult.contains("LogicalProject"));
    assertTrue(explainResult.contains("LogicalTableScan"));
  }

  @Test
  public void testFieldsProjectionChangesSchema() throws IOException {
    JSONObject result = executeQuery("source = opensearch.parquet_logs | fields ts, message");
    // Schema should only have the projected columns
    verifySchema(result, schema("ts", "timestamp"), schema("message", "keyword"));
    // Stub returns all rows (it ignores the plan), but the row data is projected
    // by column position, so row count should still be 3
    verifyNumOfRows(result, 3);
  }

  @Test
  public void testNonParquetQueryStillWorks() throws IOException {
    loadIndex(Index.ACCOUNT);
    JSONObject result =
        executeQuery(String.format("source=%s | head 1 | fields firstname", TEST_INDEX_ACCOUNT));
    assertNotNull(result);
    assertTrue(result.has("datarows"));
  }
}
