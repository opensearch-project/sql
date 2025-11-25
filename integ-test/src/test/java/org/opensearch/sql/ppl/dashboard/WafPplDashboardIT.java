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
      String mapping = TestUtils.getMappingFile("mappings/waf_logs_index_mapping.json");
      TestUtils.createIndexByRestClient(client(), WAF_LOGS_INDEX, mapping);
      TestUtils.loadDataByRestClient(
          client(),
          WAF_LOGS_INDEX,
          "src/test/java/org/opensearch/sql/ppl/dashboard/testdata/waf_logs.json");
    }
  }

  @Test
  public void testTotalRequests() throws IOException {
    String query = String.format("source=%s | stats count()", WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("count()", null, "bigint"));
    verifyDataRows(response, rows(100));
  }

  @Test
  public void testRequestsHistory() throws IOException {
    String query =
        String.format(
            "source=%s | STATS count() as Count by span(start_time, 30d), action | SORT - Count",
            WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", null, "bigint"),
        schema("span(start_time,30d)", null, "timestamp"),
        schema("action", null, "string"));
  }

  @Test
  public void testRequestsToWebACLs() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `webaclId` | sort - Count | head 10",
            WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"), schema("webaclId", null, "string"));
  }

  @Test
  public void testSources() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `httpSourceId` | sort - Count | head 5",
            WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"), schema("httpSourceId", null, "string"));
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
    verifyDataRows(
        response,
        rows(33, "US"),
        rows(8, "GB"),
        rows(7, "DE"),
        rows(7, "BR"),
        rows(6, "CA"),
        rows(5, "RU"),
        rows(3, "JP"),
        rows(3, "IN"),
        rows(3, "CN"),
        rows(3, "BE"),
        rows(2, "SG"),
        rows(2, "SE"),
        rows(2, "MX"),
        rows(2, "IE"),
        rows(2, "ES"),
        rows(2, "CH"),
        rows(2, "AU"),
        rows(1, "ZA"),
        rows(1, "PT"),
        rows(1, "NL"),
        rows(1, "IT"),
        rows(1, "FR"),
        rows(1, "FI"),
        rows(1, "CL"),
        rows(1, "AT"));
  }

  @Test
  public void testTopTerminatingRules() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `terminatingRuleId` | sort - Count, `terminatingRuleId` | head 10",
            WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Count", null, "bigint"), schema("terminatingRuleId", null, "string"));
    verifyDataRows(
        response,
        rows(13, "AWS-AWSManagedRulesAmazonIpReputationList"),
        rows(11, "XSSProtectionRule"),
        rows(11, "Default_Action"),
        rows(10, "AWS-AWSManagedRulesKnownBadInputsRuleSet"),
        rows(8, "CustomRateLimitRule"),
        rows(8, "AWS-AWSManagedRulesCommonRuleSet"),
        rows(7, "CustomIPWhitelistRule"),
        rows(7, "AWS-AWSManagedRulesSQLiRuleSet"),
        rows(7, "AWS-AWSManagedRulesLinuxRuleSet"),
        rows(5, "CSRFProtectionRule"));
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
    verifyDataRows(
        response,
        rows(5, "/api/v2/search"),
        rows(5, "/account"),
        rows(4, "/products"),
        rows(4, "/css/style.css"),
        rows(3, "/test"),
        rows(3, "/download"),
        rows(3, "/docs"),
        rows(3, "/billing"),
        rows(3, "/api/v2/users"),
        rows(2, "/about"));
  }

  @Test
  public void testTotalBlockedRequests() throws IOException {
    String query =
        String.format("source=%s | WHERE action = \\\"BLOCK\\\" | STATS count()", WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("count()", null, "bigint"));
    verifyDataRows(response, rows(21));
  }
}
