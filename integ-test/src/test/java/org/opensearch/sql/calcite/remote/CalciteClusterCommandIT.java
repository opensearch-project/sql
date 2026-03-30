/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_OTEL_LOGS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteClusterCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.BANK);
    loadIndex(Index.OTELLOGS);
  }

  @Test
  public void testBasicCluster() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'user login failed' | cluster message | fields"
                    + " cluster_label | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("cluster_label", null, "int"));
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
    verifySchema(result, schema("cluster_label", null, "int"));
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
    verifySchema(result, schema("cluster_label", null, "int"));
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
    verifySchema(result, schema("cluster_label", null, "int"));
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
    verifySchema(result, schema("my_cluster", null, "int"));
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testClusterWithShowCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'server unavailable' | cluster message"
                    + " showcount=true | fields cluster_label, cluster_count | head 1",
                TEST_INDEX_BANK));
    verifySchema(
        result, schema("cluster_label", null, "int"), schema("cluster_count", null, "bigint"));
    verifyDataRows(result, rows(1, 7));
  }

  @Test
  public void testClusterWithCustomCountField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'server unavailable' | cluster message"
                    + " countfield=my_count showcount=true | fields cluster_label, my_count"
                    + " | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("cluster_label", null, "int"), schema("my_count", null, "bigint"));
    verifyDataRows(result, rows(1, 7));
  }

  @Test
  public void testClusterWithDelimiters() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'user-login-failed' | cluster message delims='-'"
                    + " | fields cluster_label | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("cluster_label", null, "int"));
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testClusterWithAllParameters() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'system error detected' | cluster message t=0.7"
                    + " match=termset labelfield=custom_label countfield=custom_count"
                    + " showcount=true | fields custom_label, custom_count | head 1",
                TEST_INDEX_BANK));
    verifySchema(
        result, schema("custom_label", null, "int"), schema("custom_count", null, "bigint"));
    verifyDataRows(result, rows(1, 7));
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
        schema("cluster_label", null, "int"));
    verifyDataRows(result, rows(1, "system alert", 1));
  }

  @Test
  public void testClusterGroupsSimilarMessages() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = case(account_number=1, 'login failed for user"
                    + " admin', account_number=6, 'login failed for user root',"
                    + " account_number=13, 'connection timeout on server' else 'connection"
                    + " timeout on host') | cluster message match=termset showcount=true"
                    + " | fields message, cluster_label, cluster_count | sort cluster_label"
                    + " | head 4",
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("message", null, "string"),
        schema("cluster_label", null, "int"),
        schema("cluster_count", null, "bigint"));
    verifyDataRows(
        result,
        rows("login failed for user admin", 1, 2),
        rows("login failed for user root", 1, 2),
        rows("connection timeout on server", 2, 5),
        rows("connection timeout on host", 2, 5));
  }

  @Test
  public void testClusterDedupsByDefault() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = case(account_number=1, 'login failed for user"
                    + " admin', account_number=6, 'login failed for user root' else 'login"
                    + " failed for user guest') | cluster message match=termset showcount=true"
                    + " | fields message, cluster_label, cluster_count",
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("message", null, "string"),
        schema("cluster_label", null, "int"),
        schema("cluster_count", null, "bigint"));
    verifyDataRows(result, rows("login failed for user admin", 1, 7));
  }

  @Test
  public void testClusterLabelOnlyKeepsAllRows() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = case(account_number=1, 'login failed for user"
                    + " admin', account_number=6, 'login failed for user root' else 'login"
                    + " failed for user guest') | cluster message match=termset labelonly=true"
                    + " showcount=true | fields message, cluster_label, cluster_count | head 3",
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("message", null, "string"),
        schema("cluster_label", null, "int"),
        schema("cluster_count", null, "bigint"));
    verifyDataRows(
        result,
        rows("login failed for user admin", 1, 7),
        rows("login failed for user root", 1, 7),
        rows("login failed for user guest", 1, 7));
  }

  @Test
  public void testClusterNullFieldsFiltered() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = case(account_number=1, 'error occurred' else"
                    + " null) | cluster message showcount=true | fields message, cluster_label,"
                    + " cluster_count",
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("message", null, "string"),
        schema("cluster_label", null, "int"),
        schema("cluster_count", null, "bigint"));
    verifyDataRows(result, rows("error occurred", 1, 1));
  }

  @Test
  public void testClusterNullFieldsFilteredWithLabelOnly() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = case(account_number=1, 'error alpha',"
                    + " account_number=6, 'error beta' else null) | cluster message"
                    + " labelonly=true showcount=true | fields message, cluster_label,"
                    + " cluster_count | sort message | head 2",
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("message", null, "string"),
        schema("cluster_label", null, "int"),
        schema("cluster_count", null, "bigint"));
    verifyDataRows(result, rows("error alpha", 1, 2), rows("error beta", 1, 2));
  }

  @Test
  public void testClusterOnOtelLogs() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | cluster body showcount=true | fields body, cluster_label,"
                    + " cluster_count | head 3",
                TEST_INDEX_OTEL_LOGS));
    verifySchema(
        result,
        schema("body", null, "string"),
        schema("cluster_label", null, "int"),
        schema("cluster_count", null, "bigint"));
    verifyDataRows(
        result,
        rows(
            "User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart",
            1,
            1),
        rows("Payment failed: Insufficient funds for user@example.com", 2, 1),
        rows(
            "Query contains Lucene special characters: +field:value -excluded AND (grouped OR"
                + " terms) NOT \"exact phrase\" wildcard* fuzzy~2 /regex/ [range TO search]",
            3,
            1));
  }

  @Test
  public void testClusterLabelOnlyWithShowCountOnOtelLogs() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | cluster body match=termset t=0.3 labelonly=true showcount=true"
                    + " | fields body, cluster_label, cluster_count | head 3",
                TEST_INDEX_OTEL_LOGS));
    verifySchema(
        result,
        schema("body", null, "string"),
        schema("cluster_label", null, "int"),
        schema("cluster_count", null, "bigint"));
    verifyDataRows(
        result,
        rows(
            "User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart",
            1,
            3),
        rows(
            "192.168.1.1 - - [15/Jan/2024:10:30:03 +0000] \"GET"
                + " /api/products?search=laptop&category=electronics HTTP/1.1\" 200 1234 \"-\""
                + " \"Mozilla/5.0\"",
            1,
            3),
        rows(
            "[2024-01-15 10:30:09] production.INFO: User authentication successful for"
                + " admin@company.org using OAuth2",
            1,
            3));
  }
}
