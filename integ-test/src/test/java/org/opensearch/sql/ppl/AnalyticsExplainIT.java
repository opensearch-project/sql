/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.isIndexExist;
import static org.opensearch.sql.util.MatcherUtils.assertYamlEqualsIgnoreId;

import java.io.IOException;
import org.junit.Test;
import org.opensearch.client.Request;

/**
 * Explain integration tests for queries routed through the analytics engine path (Project Mustang).
 * Validates that PPL queries targeting "parquet_*" indices produce correct logical plans via the
 * _plugins/_ppl/_explain endpoint.
 *
 * <p>Expected output files are in resources/expectedOutput/analytics/. Each test compares the
 * explain YAML output against its expected file, following the same pattern as CalciteExplainIT.
 *
 * <p>Since the analytics engine is not yet available, physical and extended plans are null. Only
 * the logical plan (Calcite RelNode tree) is verified.
 */
public class AnalyticsExplainIT extends PPLIntegTestCase {

  @Override
  protected void init() throws Exception {
    // Create parquet_logs index so OpenSearchSchemaBuilder can build the schema for explain tests.
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

  private String loadAnalyticsExpectedPlan(String fileName) {
    return loadFromFile("expectedOutput/analytics/" + fileName);
  }

  @Test
  public void testExplainSimpleScan() throws IOException {
    assertYamlEqualsIgnoreId(
        loadAnalyticsExpectedPlan("explain_simple_scan.yaml"),
        explainQueryYaml("source = opensearch.parquet_logs"));
  }

  @Test
  public void testExplainProject() throws IOException {
    assertYamlEqualsIgnoreId(
        loadAnalyticsExpectedPlan("explain_project.yaml"),
        explainQueryYaml("source = opensearch.parquet_logs | fields ts, message"));
  }

  @Test
  public void testExplainFilterAndProject() throws IOException {
    assertYamlEqualsIgnoreId(
        loadAnalyticsExpectedPlan("explain_filter_project.yaml"),
        explainQueryYaml(
            "source = opensearch.parquet_logs | where status = 200 | fields ts, message"));
  }

  @Test
  public void testExplainAggregation() throws IOException {
    assertYamlEqualsIgnoreId(
        loadAnalyticsExpectedPlan("explain_aggregation.yaml"),
        explainQueryYaml("source = opensearch.parquet_logs | stats count() by status"));
  }

  @Test
  public void testExplainSort() throws IOException {
    assertYamlEqualsIgnoreId(
        loadAnalyticsExpectedPlan("explain_sort.yaml"),
        explainQueryYaml("source = opensearch.parquet_logs | sort ts"));
  }

  @Test
  public void testExplainEval() throws IOException {
    assertYamlEqualsIgnoreId(
        loadAnalyticsExpectedPlan("explain_eval.yaml"),
        explainQueryYaml("source = opensearch.parquet_logs | eval error = status = 500"));
  }
}
