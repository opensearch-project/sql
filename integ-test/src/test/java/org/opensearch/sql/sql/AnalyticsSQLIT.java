/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestUtils.isIndexExist;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
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

  private JSONObject executeSqlQuery(String sql) throws IOException {
    Request request = new Request("POST", "/_plugins/_sql");
    request.setJsonEntity("{\"query\": \"" + sql + "\"}");
    request.setOptions(RequestOptions.DEFAULT);
    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }

  @Test
  public void testSelectStarSchemaAndData() throws IOException {
    JSONObject result = executeSqlQuery("SELECT * FROM parquet_logs");
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
    JSONObject result = executeSqlQuery("SELECT status, message FROM parquet_logs");
    verifySchema(result, schema("status", "integer"), schema("message", "string"));
    verifyDataRows(
        result,
        rows("2024-01-15 10:30:00", 200),
        rows("2024-01-15 10:31:00", 200),
        rows("2024-01-15 10:32:00", 500));
  }

  @Test(expected = ResponseException.class)
  public void testSyntaxError() throws IOException {
    executeSqlQuery("SELEC * FROM parquet_logs");
  }
}
