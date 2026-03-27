/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.util.MatcherUtils.assertYamlEqualsIgnoreId;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

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

  private static final Logger LOG = LogManager.getLogger(AnalyticsExplainIT.class);

  @Override
  protected void init() throws Exception {
    // No index loading needed -- stub schema and data are hardcoded
  }

  private String loadAnalyticsExpectedPlan(String fileName) {
    return loadFromFile("expectedOutput/analytics/" + fileName);
  }

  @Test
  public void testExplainSimpleScan() throws IOException {
    String query = "source = opensearch.parquet_logs";
    String result = explainQueryYaml(query);
    LOG.info("[testExplainSimpleScan] query: {}\nresult:\n{}", query, result);
    assertYamlEqualsIgnoreId(loadAnalyticsExpectedPlan("explain_simple_scan.yaml"), result);
  }

  @Test
  public void testExplainProject() throws IOException {
    String query = "source = opensearch.parquet_logs | fields ts, message";
    String result = explainQueryYaml(query);
    LOG.info("[testExplainProject] query: {}\nresult:\n{}", query, result);
    assertYamlEqualsIgnoreId(loadAnalyticsExpectedPlan("explain_project.yaml"), result);
  }

  @Test
  public void testExplainFilterAndProject() throws IOException {
    String query = "source = opensearch.parquet_logs | where status = 200 | fields ts, message";
    String result = explainQueryYaml(query);
    LOG.info("[testExplainFilterAndProject] query: {}\nresult:\n{}", query, result);
    assertYamlEqualsIgnoreId(loadAnalyticsExpectedPlan("explain_filter_project.yaml"), result);
  }

  @Test
  public void testExplainAggregation() throws IOException {
    String query = "source = opensearch.parquet_logs | stats count() by status";
    String result = explainQueryYaml(query);
    LOG.info("[testExplainAggregation] query: {}\nresult:\n{}", query, result);
    assertYamlEqualsIgnoreId(loadAnalyticsExpectedPlan("explain_aggregation.yaml"), result);
  }

  @Test
  public void testExplainSort() throws IOException {
    String query = "source = opensearch.parquet_logs | sort ts";
    String result = explainQueryYaml(query);
    LOG.info("[testExplainSort] query: {}\nresult:\n{}", query, result);
    assertYamlEqualsIgnoreId(loadAnalyticsExpectedPlan("explain_sort.yaml"), result);
  }

  @Test
  public void testExplainEval() throws IOException {
    String query = "source = opensearch.parquet_logs | eval error = status = 500";
    String result = explainQueryYaml(query);
    LOG.info("[testExplainEval] query: {}\nresult:\n{}", query, result);
    assertYamlEqualsIgnoreId(loadAnalyticsExpectedPlan("explain_eval.yaml"), result);
  }
}
