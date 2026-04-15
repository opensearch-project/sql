/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestUtils.isIndexExist;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * Integration tests for SQL queries routed through the analytics engine path. Queries targeting
 * "parquet_*" indices are routed to {@code RestUnifiedQueryAction} which uses {@code
 * AnalyticsExecutionEngine} with a stub {@code QueryPlanExecutor}.
 *
 * <p>The stub executor returns rows in a fixed order [ts, status, message, ip_addr] regardless of
 * the plan. The schema from OpenSearchSchemaBuilder is alphabetical [ip_addr, message, status, ts].
 * AnalyticsExecutionEngine maps values by position, so the data values appear mismatched. This is
 * expected; the real analytics engine will evaluate the plan correctly.
 */
public class AnalyticsSQLIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    createParquetLogsIndex();
  }

  private void createParquetLogsIndex() throws IOException {
    if (isIndexExist(client(), "parquet_logs")) {
      return;
    }
    Request request = new Request("PUT", "/parquet_logs");
    request.setJsonEntity(
        "{"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"ts\": {\"type\": \"date\"},"
            + "    \"status\": {\"type\": \"integer\"},"
            + "    \"message\": {\"type\": \"keyword\"},"
            + "    \"ip_addr\": {\"type\": \"keyword\"}"
            + "  }"
            + "}"
            + "}");
    client().performRequest(request);
  }

  @Test
  public void testSelectStarSchemaAndData() throws IOException {
    JSONObject result = executeQuery("SELECT * FROM parquet_logs");
    verifySchema(
        result,
        schema("ip_addr", "string"),
        schema("message", "string"),
        schema("status", "integer"),
        schema("ts", "timestamp"));
    // Stub returns [ts, status, message, ip_addr] per row, mapped by position to
    // [ip_addr, message, status, ts] schema. Values appear mismatched — expected with stub.
    verifyDataRows(
        result,
        rows("2024-01-15 10:30:00", 200, "Request completed", "192.168.1.1"),
        rows("2024-01-15 10:31:00", 200, "Health check OK", "192.168.1.2"),
        rows("2024-01-15 10:32:00", 500, "Internal server error", "192.168.1.3"));
  }

  @Test
  public void testSelectSpecificColumns() throws IOException {
    JSONObject result = executeQuery("SELECT status, message FROM parquet_logs");
    verifySchema(result, schema("status", "integer"), schema("message", "string"));
    verifyDataRows(
        result,
        rows("2024-01-15 10:30:00", 200),
        rows("2024-01-15 10:31:00", 200),
        rows("2024-01-15 10:32:00", 500));
  }

  @Test
  public void testProfileResponseIncludesProfilingData() throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{\n  \"query\": \"%s\",\n  \"profile\": true\n}",
            "SELECT * FROM parquet_logs"));
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    Response response = client().performRequest(request);
    JSONObject result = new JSONObject(getResponseBody(response, true));

    assertTrue("Response should have 'profile' field when profile=true", result.has("profile"));
    JSONObject profile = result.getJSONObject("profile");

    assertTrue("Profile should have 'summary' field", profile.has("summary"));
    double totalTime = profile.getJSONObject("summary").getDouble("total_time_ms");
    assertTrue("Total time should be > 0, got " + totalTime + "ms", totalTime > 0);

    JSONObject phases = profile.getJSONObject("phases");

    double analyzeTime = phases.getJSONObject("analyze").getDouble("time_ms");
    assertTrue("Analyze phase should be > 0, got " + analyzeTime + "ms", analyzeTime > 0);

    double executeTime = phases.getJSONObject("execute").getDouble("time_ms");
    assertTrue("Execute phase should be > 0, got " + executeTime + "ms", executeTime > 0);

    double formatTime = phases.getJSONObject("format").getDouble("time_ms");
    assertTrue("Format phase should be > 0, got " + formatTime + "ms", formatTime > 0);
  }

  @Test
  public void testProfileDisabledByDefault() throws IOException {
    JSONObject result = executeQuery("SELECT * FROM parquet_logs");
    assertFalse("Response should NOT have 'profile' field by default", result.has("profile"));
  }

  @Test
  public void testProfileExplicitlyDisabled() throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{\n  \"query\": \"%s\",\n  \"profile\": false\n}",
            "SELECT * FROM parquet_logs"));
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    Response response = client().performRequest(request);
    JSONObject result = new JSONObject(getResponseBody(response, true));

    assertFalse("Response should NOT have 'profile' when profile=false", result.has("profile"));
  }

  @Test(expected = ResponseException.class)
  public void testSyntaxError() throws IOException {
    executeQuery("SELEC * FROM parquet_logs");
  }
}
