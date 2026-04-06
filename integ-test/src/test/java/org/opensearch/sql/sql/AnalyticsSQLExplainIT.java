/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestUtils.isIndexExist;
import static org.opensearch.sql.util.MatcherUtils.assertJsonEqualsIgnoreId;

import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * Explain integration tests for SQL queries routed through the analytics engine path (Project
 * Analytics engine). Validates that SQL queries targeting "parquet_*" indices produce correct
 * logical plans via the _plugins/_sql/_explain endpoint.
 *
 * <p>Expected output files are in resources/expectedOutput/analytics_sql/. Each test compares the
 * explain JSON output against its expected file.
 */
@SuppressWarnings("deprecation") // assertJsonEqualsIgnoreId is correct for JSON explain response
public class AnalyticsSQLExplainIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    if (!isIndexExist(client(), "parquet_logs")) {
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
  }

  private String explainSqlQuery(String sql) throws IOException {
    Request request = new Request("POST", "/_plugins/_sql/_explain");
    request.setJsonEntity("{\"query\": \"" + sql + "\"}");
    request.setOptions(RequestOptions.DEFAULT);
    Response response = client().performRequest(request);
    return getResponseBody(response);
  }

  private static String loadExpectedJson(String fileName) {
    return loadFromFile("expectedOutput/analytics_sql/" + fileName);
  }

  private static String loadFromFile(String filename) {
    try {
      URI uri = Resources.getResource(filename).toURI();
      return new String(Files.readAllBytes(Paths.get(uri)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testExplainSelectStar() throws IOException {
    assertJsonEqualsIgnoreId(
        loadExpectedJson("explain_select_star.json"),
        explainSqlQuery("SELECT * FROM parquet_logs"));
  }

  @Test
  public void testExplainSelectColumns() throws IOException {
    assertJsonEqualsIgnoreId(
        loadExpectedJson("explain_select_columns.json"),
        explainSqlQuery("SELECT ts, status FROM parquet_logs"));
  }

  @Test
  public void testExplainSelectWithWhere() throws IOException {
    assertJsonEqualsIgnoreId(
        loadExpectedJson("explain_select_where.json"),
        explainSqlQuery("SELECT ts, message FROM parquet_logs WHERE status = 200"));
  }
}
