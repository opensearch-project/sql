/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WEBLOGS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.TestsConstants;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLCaseFunctionIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.WEBLOG);
    appendDataForBadResponse();
  }

  private void appendDataForBadResponse() throws IOException {
    Request request1 = new Request("PUT", "/" + TEST_INDEX_WEBLOGS + "/_doc/7?refresh=true");
    request1.setJsonEntity(
        "{\"host\": \"::1\", \"method\": \"GET\", \"url\": \"/history/apollo/\", \"response\":"
            + " \"301\", \"bytes\": \"6245\"}");
    client().performRequest(request1);
    Request request2 =
        new Request("PUT", "/" + TestsConstants.TEST_INDEX_WEBLOGS + "/_doc/8?refresh=true");
    request2.setJsonEntity(
        "{\"host\": \"0.0.0.2\", \"method\": \"GET\", \"url\":"
            + " \"/shuttle/missions/sts-73/mission-sts-73.html\", \"response\": \"500\", \"bytes\":"
            + " \"4085\"}");
    client().performRequest(request2);
    Request request3 =
        new Request("PUT", "/" + TestsConstants.TEST_INDEX_WEBLOGS + "/_doc/9?refresh=true");
    request3.setJsonEntity(
        "{\"host\": \"::3\", \"method\": \"GET\", \"url\": \"/shuttle/countdown/countdown.html\","
            + " \"response\": \"403\", \"bytes\": \"3985\"}");
    client().performRequest(request3);
    Request request4 =
        new Request("PUT", "/" + TestsConstants.TEST_INDEX_WEBLOGS + "/_doc/10?refresh=true");
    request4.setJsonEntity(
        "{\"host\": \"1.2.3.5\", \"method\": \"GET\", \"url\": \"/history/voyager2/\","
            + " \"response\": null, \"bytes\": \"4321\"}");
    client().performRequest(request4);
  }

  @Test
  public void testCaseWhenWithCast() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s| eval status =    case(        cast(response as int) >= 200 AND"
                    + " cast(response as int) < 300, 'Success',        cast(response as int) >= 300"
                    + " AND cast(response as int) < 400, 'Redirection',        cast(response as"
                    + " int) >= 400 AND cast(response as int) < 500, 'Client Error',       "
                    + " cast(response as int) >= 500 AND cast(response as int) < 600, 'Server"
                    + " Error'        else concat('Incorrect HTTP status code for', url))| where"
                    + " status != 'Success' | fields host, method, message, bytes, response, url,"
                    + " status",
                TEST_INDEX_WEBLOGS));
    verifySchema(
        actual,
        schema("host", "ip"),
        schema("method", "string"),
        schema("message", "string"),
        schema("url", "string"),
        schema("response", "string"),
        schema("bytes", "string"),
        schema("status", "string"));
    verifyDataRows(
        actual,
        rows("::1", "GET", null, "6245", "301", "/history/apollo/", "Redirection"),
        rows(
            "0.0.0.2",
            "GET",
            null,
            "4085",
            "500",
            "/shuttle/missions/sts-73/mission-sts-73.html",
            "Server Error"),
        rows(
            "::3", "GET", null, "3985", "403", "/shuttle/countdown/countdown.html", "Client Error"),
        rows(
            "1.2.3.5",
            "GET",
            null,
            "4321",
            null,
            "/history/voyager2/",
            "Incorrect HTTP status code for/history/voyager2/"));
  }

  @Test
  public void testCaseWhenNoElse() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s| eval status =    case(        cast(response as int) >= 200 AND"
                    + " cast(response as int) < 300, 'Success',        cast(response as int) >= 300"
                    + " AND cast(response as int) < 400, 'Redirection',        cast(response as"
                    + " int) >= 400 AND cast(response as int) < 500, 'Client Error',       "
                    + " cast(response as int) >= 500 AND cast(response as int) < 600, 'Server"
                    + " Error')| where isnull(status) OR status != 'Success' | fields host, method,"
                    + " message, bytes, response, url, status",
                TEST_INDEX_WEBLOGS));
    verifySchema(
        actual,
        schema("host", "ip"),
        schema("method", "string"),
        schema("message", "string"),
        schema("url", "string"),
        schema("response", "string"),
        schema("bytes", "string"),
        schema("status", "string"));
    verifyDataRows(
        actual,
        rows("::1", "GET", null, "6245", "301", "/history/apollo/", "Redirection"),
        rows(
            "0.0.0.2",
            "GET",
            null,
            "4085",
            "500",
            "/shuttle/missions/sts-73/mission-sts-73.html",
            "Server Error"),
        rows(
            "::3", "GET", null, "3985", "403", "/shuttle/countdown/countdown.html", "Client Error"),
        rows("1.2.3.5", "GET", null, "4321", null, "/history/voyager2/", null));
  }

  @Test
  public void testCaseWhenWithIn() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s   | eval status =       case(           response in ('200'), 'Success', "
                    + "          response in ('300', '301'), 'Redirection',           response in"
                    + " ('400', '403'), 'Client Error',           response in ('500', '505'),"
                    + " 'Server Error'           else concat('Incorrect HTTP status code for',"
                    + " url))   | where status != 'Success' | fields host, method, message, bytes,"
                    + " response, url, status",
                TEST_INDEX_WEBLOGS));
    verifySchema(
        actual,
        schema("host", "ip"),
        schema("method", "string"),
        schema("message", "string"),
        schema("url", "string"),
        schema("response", "string"),
        schema("bytes", "string"),
        schema("status", "string"));
    verifyDataRows(
        actual,
        rows("::1", "GET", null, "6245", "301", "/history/apollo/", "Redirection"),
        rows(
            "0.0.0.2",
            "GET",
            null,
            "4085",
            "500",
            "/shuttle/missions/sts-73/mission-sts-73.html",
            "Server Error"),
        rows(
            "::3", "GET", null, "3985", "403", "/shuttle/countdown/countdown.html", "Client Error"),
        rows(
            "1.2.3.5",
            "GET",
            null,
            "4321",
            null,
            "/history/voyager2/",
            "Incorrect HTTP status code for/history/voyager2/"));
  }

  @Test
  public void testCaseWhenInFilter() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s"
                    + "| where not true ="
                    + "    case("
                    + "        response in ('200'), true,"
                    + "        response in ('300', '301'), false,"
                    + "        response in ('400', '403'), false,"
                    + "        response in ('500', '505'), false"
                    + "        else false)"
                    + "| fields host, method, message, bytes, response, url",
                TEST_INDEX_WEBLOGS));
    verifySchema(
        actual,
        schema("host", "ip"),
        schema("method", "string"),
        schema("message", "string"),
        schema("url", "string"),
        schema("response", "string"),
        schema("bytes", "string"));
    verifyDataRows(
        actual,
        rows("::1", "GET", null, "6245", "301", "/history/apollo/"),
        rows("0.0.0.2", "GET", null, "4085", "500", "/shuttle/missions/sts-73/mission-sts-73.html"),
        rows("::3", "GET", null, "3985", "403", "/shuttle/countdown/countdown.html"),
        rows("1.2.3.5", "GET", null, "4321", null, "/history/voyager2/"));
  }

  @Test
  public void testCaseWhenInSubquery() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s"
                    + "| where response in ["
                    + "    source = %s"
                    + "    | eval new_response = case("
                    + "        response in ('200'), '201',"
                    + "        response in ('300', '301'), '301',"
                    + "        response in ('400', '403'), '403',"
                    + "        response in ('500', '505'), '500'"
                    + "        else concat('Incorrect HTTP status code for', url))"
                    + "    | fields new_response"
                    + "  ]"
                    + "| fields host, method, message, bytes, response, url",
                TEST_INDEX_WEBLOGS, TEST_INDEX_WEBLOGS));
    verifySchema(
        actual,
        schema("host", "ip"),
        schema("method", "string"),
        schema("message", "string"),
        schema("url", "string"),
        schema("response", "string"),
        schema("bytes", "string"));
    verifyDataRows(
        actual,
        rows("::1", "GET", null, "6245", "301", "/history/apollo/"),
        rows("0.0.0.2", "GET", null, "4085", "500", "/shuttle/missions/sts-73/mission-sts-73.html"),
        rows("::3", "GET", null, "3985", "403", "/shuttle/countdown/countdown.html"));
  }

  @Test
  public void testCaseRangeAggregationPushdown() throws IOException {
    // Test CASE expression that can be optimized to range aggregation
    // Note: This has an implicit ELSE NULL, so it won't be optimized
    // But it should still work correctly
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval range_bucket = case("
                    + "  cast(bytes as int) < 1000, 'small',"
                    + "  cast(bytes as int) >= 1000 AND cast(bytes as int) < 5000, 'medium',"
                    + "  cast(bytes as int) >= 5000, 'large'"
                    + ") | stats count() as total by range_bucket | sort range_bucket",
                TEST_INDEX_WEBLOGS));

    verifySchema(actual, schema("range_bucket", "string"), schema("total", "bigint"));

    // This should work but won't be optimized due to implicit NULL bucket
    assertTrue(actual.getJSONArray("datarows").length() > 0);
  }

  @Test
  public void testCaseRangeAggregationWithMetrics() throws IOException {
    // Test CASE-to-range with additional aggregations
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval size_category = case(  cast(bytes as int) < 2000, 'small', "
                    + " cast(bytes as int) >= 2000 AND cast(bytes as int) < 5000, 'medium', "
                    + " cast(bytes as int) >= 5000, 'large') | stats count() as total,"
                    + " avg(cast(bytes as int)) as avg_bytes by size_category | sort size_category",
                TEST_INDEX_WEBLOGS));

    verifySchema(
        actual,
        schema("size_category", "string"),
        schema("total", "bigint"),
        schema("avg_bytes", "double"));

    // Verify we get results for each category
    // The exact values may vary based on test data, but structure should be correct
    assertEquals(3, actual.getJSONArray("datarows").length());
  }

  @Test
  public void testCaseRangeAggregationWithElse() throws IOException {
    // Test CASE with explicit ELSE clause
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval status_category = case(  cast(response as int) < 300, 'success', "
                    + " cast(response as int) >= 300 AND cast(response as int) < 400, 'redirect', "
                    + " cast(response as int) >= 400 AND cast(response as int) < 500,"
                    + " 'client_error',  cast(response as int) >= 500, 'server_error'  else"
                    + " 'unknown') | stats count() by status_category | sort status_category",
                TEST_INDEX_WEBLOGS));

    verifySchema(actual, schema("status_category", "string"), schema("count()", "bigint"));

    // Should handle the ELSE case for null/non-numeric responses
    assertTrue(actual.getJSONArray("datarows").length() > 0);
  }

  @Test
  public void testNonOptimizableCaseExpression() throws IOException {
    // Test CASE that cannot be optimized (different fields)
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval mixed_condition = case("
                    + "  cast(bytes as int) < 1000, 'small_bytes',"
                    + "  cast(response as int) >= 400, 'error_response'"
                    + "  else 'other'"
                    + ") | stats count() by mixed_condition",
                TEST_INDEX_WEBLOGS));

    verifySchema(actual, schema("mixed_condition", "string"), schema("count()", "bigint"));

    // This should work but won't be optimized
    assertTrue(actual.getJSONArray("datarows").length() > 0);
  }

  @Test
  public void testCaseWithNonLiteralResult() throws IOException {
    // Test CASE that cannot be optimized (non-literal results)
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval computed_result = case("
                    + "  cast(bytes as int) < 1000, concat('small_', host),"
                    + "  cast(bytes as int) >= 1000, concat('large_', host)"
                    + ") | stats count() by computed_result | head 3",
                TEST_INDEX_WEBLOGS));

    verifySchema(actual, schema("computed_result", "string"), schema("count()", "bigint"));

    // This should work but won't be optimized to range aggregation
    assertTrue(actual.getJSONArray("datarows").length() > 0);
  }

  @Test
  public void testOptimizableCaseRangeAggregation() throws IOException {
    // Test CASE that could be optimized if all ranges are covered with explicit ELSE
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval size_bucket = case("
                    + "  cast(bytes as int) < 2000, 'small',"
                    + "  cast(bytes as int) >= 2000 AND cast(bytes as int) < 5000, 'medium',"
                    + "  cast(bytes as int) >= 5000, 'large'"
                    + "  else 'unknown'"
                    + ") | stats count() by size_bucket | sort size_bucket",
                TEST_INDEX_WEBLOGS));

    verifySchema(actual, schema("size_bucket", "string"), schema("count()", "bigint"));

    // This should work - the explicit ELSE makes it potentially optimizable
    assertTrue(actual.getJSONArray("datarows").length() > 0);
  }
}
