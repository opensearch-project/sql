/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.ResponseException;

/**
 * Integration tests for PPL queries routed through the analytics engine path (Project Mustang).
 * Queries targeting "parquet_*" indices are routed to {@code RestUnifiedQueryAction} which uses
 * {@code AnalyticsExecutionEngine} with a stub {@code QueryPlanExecutor}.
 *
 * <p>These tests validate the full pipeline: REST request -> routing -> planning via
 * UnifiedQueryPlanner -> execution via AnalyticsExecutionEngine -> response formatting.
 *
 * <p>The stub executor always returns the full table rows regardless of the logical plan. After
 * projection (| fields), the execution engine maps row values by position -- so projected columns
 * get the values from the corresponding positions in the full row, not the actual projected column.
 * This is expected behavior for a stub; the real analytics engine will evaluate the plan correctly.
 */
public class AnalyticsPPLIT extends PPLIntegTestCase {

  private static final Logger LOG = LogManager.getLogger(AnalyticsPPLIT.class);

  @Override
  protected void init() throws Exception {
    // No index loading needed -- stub schema and data are hardcoded
    // in RestUnifiedQueryAction and StubQueryPlanExecutor
  }

  // --- Full table scan tests with schema + data verification ---

  @Test
  public void testBasicQuerySchemaAndData() throws IOException {
    String query = "source = opensearch.parquet_logs";
    JSONObject result = executeQuery(query);
    LOG.info("[testBasicQuerySchemaAndData] query: {}\nresponse: {}", query, result.toString(2));

    verifySchema(
        result,
        schema("ts", "timestamp"),
        schema("status", "integer"),
        schema("message", "keyword"),
        schema("ip_addr", "keyword"));
    verifyNumOfRows(result, 3);
    verifyDataRows(
        result,
        rows("2024-01-15 10:30:00", 200, "Request completed", "192.168.1.1"),
        rows("2024-01-15 10:31:00", 200, "Health check OK", "192.168.1.2"),
        rows("2024-01-15 10:32:00", 500, "Internal server error", "192.168.1.3"));
  }

  @Test
  public void testParquetMetricsSchemaAndData() throws IOException {
    String query = "source = opensearch.parquet_metrics";
    JSONObject result = executeQuery(query);
    LOG.info(
        "[testParquetMetricsSchemaAndData] query: {}\nresponse: {}", query, result.toString(2));

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
    String query = "source = opensearch.parquet_logs";
    JSONObject result = executeQuery(query);
    LOG.info(
        "[testResponseFormatHasRequiredFields] query: {}\nresponse: {}", query, result.toString(2));

    String msg = "Full response: " + result.toString(2);
    assertTrue("Response missing 'schema'. " + msg, result.has("schema"));
    assertTrue("Response missing 'datarows'. " + msg, result.has("datarows"));
    assertTrue("Response missing 'total'. " + msg, result.has("total"));
    assertTrue("Response missing 'size'. " + msg, result.has("size"));
    assertTrue("Response missing 'status'. " + msg, result.has("status"));
    assertEquals(
        "Expected status 200 but got " + result.getInt("status") + ". " + msg,
        200,
        result.getInt("status"));
  }

  @Test
  public void testTotalAndSizeMatchRowCount() throws IOException {
    String query = "source = opensearch.parquet_logs";
    JSONObject result = executeQuery(query);
    LOG.info("[testTotalAndSizeMatchRowCount] query: {}\nresponse: {}", query, result.toString(2));

    int rowCount = result.getJSONArray("datarows").length();
    assertEquals(
        String.format(
            "total should match row count. rows=%d, total=%d, size=%d. Response: %s",
            rowCount, result.getInt("total"), result.getInt("size"), result.toString(2)),
        rowCount,
        result.getInt("total"));
    assertEquals(
        String.format(
            "size should match row count. rows=%d, size=%d. Response: %s",
            rowCount, result.getInt("size"), result.toString(2)),
        rowCount,
        result.getInt("size"));
  }

  // --- Projection tests (schema verification -- stub doesn't evaluate projections) ---

  @Test
  public void testFieldsProjectionChangesSchema() throws IOException {
    String query = "source = opensearch.parquet_logs | fields ts, message";
    JSONObject result = executeQuery(query);
    LOG.info(
        "[testFieldsProjectionChangesSchema] query: {}\nresponse: {}", query, result.toString(2));

    verifySchema(result, schema("ts", "timestamp"), schema("message", "keyword"));
    verifyNumOfRows(result, 3);
  }

  @Test
  public void testSingleFieldProjection() throws IOException {
    String query = "source = opensearch.parquet_logs | fields status";
    JSONObject result = executeQuery(query);
    LOG.info("[testSingleFieldProjection] query: {}\nresponse: {}", query, result.toString(2));

    verifySchema(result, schema("status", "integer"));
    verifyNumOfRows(result, 3);
  }

  // --- Error handling tests ---

  @Test
  public void testSyntaxErrorReturnsClientError() throws IOException {
    String query = "source = opensearch.parquet_logs | invalid_command";
    ResponseException e = assertThrows(ResponseException.class, () -> executeQuery(query));
    int statusCode = e.getResponse().getStatusLine().getStatusCode();
    String responseBody = getResponseBody(e.getResponse(), true);
    LOG.info(
        "[testSyntaxErrorReturnsClientError] query: {}\nstatus: {}\nresponse: {}",
        query,
        statusCode,
        responseBody);

    assertTrue(
        String.format(
            "Syntax error should return 4xx, got %d. Response: %s", statusCode, responseBody),
        statusCode >= 400 && statusCode < 500);
  }

  // --- Regression tests ---

  @Test
  public void testNonParquetQueryStillWorks() throws IOException {
    loadIndex(Index.ACCOUNT);
    String query = String.format("source=%s | head 1 | fields firstname", TEST_INDEX_ACCOUNT);
    JSONObject result = executeQuery(query);
    LOG.info("[testNonParquetQueryStillWorks] query: {}\nresponse: {}", query, result.toString(2));

    assertNotNull("Non-parquet query returned null. Query: " + query, result);
    assertTrue(
        "Non-parquet query missing 'datarows'. Response: " + result.toString(2),
        result.has("datarows"));
    int rowCount = result.getJSONArray("datarows").length();
    assertTrue(
        String.format(
            "Non-parquet query returned 0 rows. Expected > 0. Response: %s", result.toString(2)),
        rowCount > 0);
  }

  @Test
  public void testNonParquetAggregationStillWorks() throws IOException {
    loadIndex(Index.ACCOUNT);
    String query = String.format("source=%s | stats count()", TEST_INDEX_ACCOUNT);
    JSONObject result = executeQuery(query);
    LOG.info(
        "[testNonParquetAggregationStillWorks] query: {}\nresponse: {}", query, result.toString(2));

    int total = result.getInt("total");
    assertTrue(
        String.format(
            "Non-parquet aggregation returned total=%d, expected > 0. Response: %s",
            total, result.toString(2)),
        total > 0);
  }
}
