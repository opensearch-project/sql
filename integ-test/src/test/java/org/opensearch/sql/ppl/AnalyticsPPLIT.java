/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
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

  @Override
  protected void init() throws Exception {
    // No index loading needed -- stub schema and data are hardcoded
    // in RestUnifiedQueryAction and StubQueryPlanExecutor
  }

  // --- Full table scan tests with schema + data verification ---

  @Test
  public void testBasicQuerySchemaAndData() throws IOException {
    JSONObject result = executeQuery("source = opensearch.parquet_logs");
    verifySchema(
        result,
        schema("ts", "timestamp"),
        schema("status", "int"),
        schema("message", "string"),
        schema("ip_addr", "string"));
    verifyNumOfRows(result, 3);
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
        schema("host", "string"));
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
    assertTrue("Response missing 'schema'", result.has("schema"));
    assertTrue("Response missing 'datarows'", result.has("datarows"));
    assertTrue("Response missing 'total'", result.has("total"));
    assertTrue("Response missing 'size'", result.has("size"));
  }

  @Test
  public void testTotalAndSizeMatchRowCount() throws IOException {
    JSONObject result = executeQuery("source = opensearch.parquet_logs");
    int rowCount = result.getJSONArray("datarows").length();
    assertEquals(rowCount, result.getInt("total"));
    assertEquals(rowCount, result.getInt("size"));
  }

  // --- Projection tests (schema verification -- stub doesn't evaluate projections) ---

  @Test
  public void testFieldsProjectionChangesSchema() throws IOException {
    JSONObject result = executeQuery("source = opensearch.parquet_logs | fields ts, message");
    verifySchema(result, schema("ts", "timestamp"), schema("message", "string"));
    verifyNumOfRows(result, 3);
  }

  @Test
  public void testSingleFieldProjection() throws IOException {
    JSONObject result = executeQuery("source = opensearch.parquet_logs | fields status");
    verifySchema(result, schema("status", "int"));
    verifyNumOfRows(result, 3);
  }

  // --- Profiling tests ---

  @Test
  public void testProfileResponseIncludesProfilingData() throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{\n  \"query\": \"%s\",\n  \"profile\": true\n}",
            "source = opensearch.parquet_logs"));
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    Response response = client().performRequest(request);
    JSONObject result = new JSONObject(getResponseBody(response, true));

    assertTrue("Response should have 'profile' field when profile=true", result.has("profile"));
    JSONObject profile = result.getJSONObject("profile");
    assertTrue("Profile should have 'phases' field", profile.has("phases"));
    JSONObject phases = profile.getJSONObject("phases");

    assertTrue("Phases should have 'analyze' field", phases.has("analyze"));
    double analyzeTime = phases.getJSONObject("analyze").getDouble("time_ms");
    assertTrue(
        "Analyze phase should have non-zero time, got " + analyzeTime + "ms", analyzeTime > 0);

    assertTrue("Phases should have 'execute' field", phases.has("execute"));
    double executeTime = phases.getJSONObject("execute").getDouble("time_ms");
    assertTrue(
        "Execute phase should have non-zero time, got " + executeTime + "ms", executeTime > 0);
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
    assertNotNull(result);
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
