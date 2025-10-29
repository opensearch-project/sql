/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.dashboard;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.legacy.TestUtils;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for WAF PPL dashboard queries. These tests ensure that WAF-related PPL queries
 * work correctly with actual test data.
 */
public class WafPplDashboardIT extends PPLIntegTestCase {

  private static final String WAF_LOGS_INDEX = "waf_logs";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadWafLogsIndex();
  }

  private void loadWafLogsIndex() throws IOException {
    if (!TestUtils.isIndexExist(client(), WAF_LOGS_INDEX)) {
      String mapping = TestUtils.getMappingFile("indexDefinitions/waf_logs_index_mapping.json");
      TestUtils.createIndexByRestClient(client(), WAF_LOGS_INDEX, mapping);
      TestUtils.loadDataByRestClient(client(), WAF_LOGS_INDEX, "src/test/resources/waf_logs.json");
    }
  }

  @Test
  public void testTotalRequests() throws IOException {
    String query = String.format("source=%s | stats count()", WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("count()", null, "bigint"));
    verifyDataRows(response, rows(3));
  }

  @Test
  public void testRequestsHistory() throws IOException {
    String query =
        String.format(
            "source=%s | STATS count() as Count by span(timestamp, 30d), action | SORT - Count",
            WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", null, "bigint"),
        schema("span(timestamp,30d)", null, "bigint"),
        schema("action", null, "string"));
    verifyDataRows(response, rows(2, 1731456000000L, "BLOCK"), rows(1, 1731456000000L, "ALLOW"));
  }

  @Test
  public void testRequestsToWebACLs() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `webaclId` | sort - Count | head 3",
            WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"), schema("webaclId", null, "string"));
    verifyDataRows(
        response,
        rows(
            1,
            "arn:aws:wafv2:us-west-2:111111111111:regional/webacl/TestWAF-pdx/12345678-1234-1234-1234-123456789012"),
        rows(
            1,
            "arn:aws:wafv2:us-west-2:222222222222:regional/webacl/TestWAF-pdx/12345678-1234-1234-1234-123456789012"),
        rows(
            1,
            "arn:aws:wafv2:us-west-2:333333333333:regional/webacl/TestWAF-pdx/12345678-1234-1234-1234-123456789012"));
  }

  @Test
  public void testSources() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `httpSourceId` | sort - Count | head 5",
            WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"), schema("httpSourceId", null, "string"));
    verifyDataRows(
        response,
        rows(1, "111111111111:yhltew7mtf:dev"),
        rows(1, "222222222222:yhltew7mtf:dev"),
        rows(1, "333333333333:yhltew7mtf:dev"));
  }

  @Test
  public void testTopClientIPs() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `httpRequest.clientIp` | sort - Count |"
                + " head 10",
            WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Count", null, "bigint"), schema("httpRequest.clientIp", null, "string"));
    verifyDataRows(
        response, rows(1, "149.165.180.212"), rows(1, "121.236.106.18"), rows(1, "108.166.91.31"));
  }

  @Test
  public void testTopCountries() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `httpRequest.country` | sort - Count ",
            WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Count", null, "bigint"), schema("httpRequest.country", null, "string"));
    verifyDataRows(response, rows(1, "GY"), rows(1, "MX"), rows(1, "PN"));
  }

  @Test
  public void testTopTerminatingRules() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `terminatingRuleId` | sort - Count | head 10",
            WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Count", null, "bigint"), schema("terminatingRuleId", null, "string"));
    verifyDataRows(response, rows(2, "RULE_ID_3"), rows(1, "RULE_ID_7"));
  }

  @Test
  public void testTopRequestURIs() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `httpRequest.uri` | sort - Count | head 10",
            WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Count", null, "bigint"), schema("httpRequest.uri", null, "string"));
    verifyDataRows(response, rows(3, "/example-path"));
  }

  @Test
  public void testTotalBlockedRequests() throws IOException {
    String query =
        String.format("source=%s | WHERE action = \\\"BLOCK\\\" | STATS count()", WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("count()", null, "bigint"));
    verifyDataRows(response, rows(2));
  }
}
