/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.legacy.TestUtils;

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
      String mapping = TestUtils.getMappingFile("waf_logs_index_mapping.json");
      TestUtils.createIndexByRestClient(client(), WAF_LOGS_INDEX, mapping);
      TestUtils.loadDataByRestClient(client(), WAF_LOGS_INDEX, "src/test/resources/waf_logs.json");
    }
  }

  @Test
  public void testTotalRequests() throws IOException {
    String query = String.format("source=%s | stats count() as Count", WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"));
    verifyDataRows(response, rows(3));
  }

  @Test
  public void testRequestsHistory() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() by `@timestamp`, `aws.waf.action`", WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("count()", null, "bigint"),
        schema("@timestamp", null, "timestamp"),
        schema("aws.waf.action", null, "string"));
    verifyDataRows(
        response,
        rows(1, "2024-01-15 10:00:00", "BLOCK"),
        rows(1, "2024-01-15 10:00:05", "ALLOW"),
        rows(1, "2024-01-15 10:00:10", "BLOCK"));
  }

  @Test
  public void testRequestsToWebACLs() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `aws.waf.webaclId` | sort - Count | head 10",
            WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Count", null, "bigint"), schema("aws.waf.webaclId", null, "string"));
    verifyDataRows(
        response,
        rows(
            1,
            "arn:aws:wafv2:us-west-2:269290782541:regional/webacl/TestWAF-pdx/bfeae622-3df5-4fbe-a377-329e3518a60a"),
        rows(
            1,
            "arn:aws:wafv2:us-west-2:107897355464:regional/webacl/TestWAF-pdx/bfeae622-3df5-4fbe-a377-329e3518a60a"),
        rows(
            1,
            "arn:aws:wafv2:us-west-2:167022069071:regional/webacl/TestWAF-pdx/bfeae622-3df5-4fbe-a377-329e3518a60a"));
  }

  @Test
  public void testSources() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `aws.waf.httpSourceId` | sort - Count | head 5",
            WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response, schema("Count", null, "bigint"), schema("aws.waf.httpSourceId", null, "string"));
    verifyDataRows(
        response,
        rows(1, "269290782541:yhltew7mtf:dev"),
        rows(1, "107897355464:yhltew7mtf:dev"),
        rows(1, "167022069071:yhltew7mtf:dev"));
  }

  @Test
  public void testTopClientIPs() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `aws.waf.httpRequest.clientIp` | sort - Count |"
                + " head 10",
            WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", null, "bigint"),
        schema("aws.waf.httpRequest.clientIp", null, "string"));
    verifyDataRows(
        response, rows(1, "149.165.180.212"), rows(1, "121.236.106.18"), rows(1, "108.166.91.31"));
  }

  @Test
  public void testTopCountries() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `aws.waf.httpRequest.country` | sort - Count",
            WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", null, "bigint"),
        schema("aws.waf.httpRequest.country", null, "string"));
    verifyDataRows(response, rows(1, "GY"), rows(1, "MX"), rows(1, "PN"));
  }

  @Test
  public void testTopTerminatingRules() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `aws.waf.terminatingRuleId` | sort - Count |"
                + " head 10",
            WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", null, "bigint"),
        schema("aws.waf.terminatingRuleId", null, "string"));
    verifyDataRows(response, rows(2, "RULE_ID_3"), rows(1, "RULE_ID_7"));
  }

  @Test
  public void testTopRequestURIs() throws IOException {
    String query =
        String.format(
            "source=%s | stats count() as Count by `aws.waf.httpRequest.uri` | sort - Count | head"
                + " 10",
            WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema("Count", null, "bigint"),
        schema("aws.waf.httpRequest.uri", null, "string"));
    verifyDataRows(response, rows(3, "/example-path"));
  }

  @Test
  public void testTotalBlockedRequests() throws IOException {
    String query =
        String.format(
            "source=%s | stats sum(if(`aws.waf.action` = 'BLOCK', 1, 0)) as Count", WAF_LOGS_INDEX);

    JSONObject response = executeQuery(query);
    verifySchema(response, schema("Count", null, "bigint"));
    verifyDataRows(response, rows(2));
  }
}
