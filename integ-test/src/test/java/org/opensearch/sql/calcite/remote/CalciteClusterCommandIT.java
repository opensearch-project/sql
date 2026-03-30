/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteClusterCommandIT extends PPLIntegTestCase {

  @Test
  public void testBasicCluster() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'user login failed' | cluster message | fields"
                    + " cluster_label | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("cluster_label", null, "integer"));
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testClusterWithCustomThreshold() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'error connecting to database' | cluster message"
                    + " t=0.8 | fields cluster_label | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("cluster_label", null, "integer"));
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testClusterWithTermsetMatch() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'user authentication failed' | cluster message"
                    + " match=termset | fields cluster_label | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("cluster_label", null, "integer"));
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testClusterWithNgramsetMatch() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'connection timeout error' | cluster message"
                    + " match=ngramset | fields cluster_label | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("cluster_label", null, "integer"));
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testClusterWithCustomLabelField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'database error occurred' | cluster message"
                    + " labelfield=my_cluster | fields my_cluster | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("my_cluster", null, "integer"));
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testClusterWithCountField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'server unavailable' | cluster message"
                    + " countfield=cluster_count | fields cluster_label, cluster_count | head 1",
                TEST_INDEX_BANK));
    verifySchema(
        result, schema("cluster_label", null, "integer"), schema("cluster_count", null, "integer"));
    verifyDataRows(result, rows(1, 1));
  }

  @Test
  public void testClusterWithMultipleMessages() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = case(account_number=1, 'login failed',"
                    + " account_number=6, 'login error', 'connection timeout') | cluster message |"
                    + " fields message, cluster_label | head 3",
                TEST_INDEX_BANK));
    verifySchema(
        result, schema("message", null, "string"), schema("cluster_label", null, "integer"));
    // Similar messages "login failed" and "login error" should cluster together
    // Different message "connection timeout" should get different cluster
    verifyDataRows(
        result, rows("login failed", 1), rows("login error", 1), rows("connection timeout", 2));
  }

  @Test
  public void testClusterWithAllParameters() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'system error detected' | cluster message t=0.7"
                    + " match=termset labelfield=custom_label countfield=custom_count | fields"
                    + " custom_label, custom_count | head 1",
                TEST_INDEX_BANK));
    verifySchema(
        result, schema("custom_label", null, "integer"), schema("custom_count", null, "integer"));
    verifyDataRows(result, rows(1, 1));
  }

  @Test
  public void testClusterWithDelimiters() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'user-login-failed' | cluster message delims='-'"
                    + " | fields cluster_label | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("cluster_label", null, "integer"));
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testClusterPreservesOtherFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'system alert' | cluster message | fields"
                    + " account_number, message, cluster_label | head 1",
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("account_number", null, "bigint"),
        schema("message", null, "string"),
        schema("cluster_label", null, "integer"));
    // Should preserve original fields along with cluster results
    verifyDataRows(result, rows(1, "system alert", 1));
  }
}
