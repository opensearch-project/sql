/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_OTEL_LOGS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY_WITH_NULL;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WEBLOGS;
import static org.opensearch.sql.util.MatcherUtils.closeTo;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Assume;
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
    loadIndex(Index.TIME_TEST_DATA);
    loadIndex(Index.STATE_COUNTRY_WITH_NULL);
    loadIndex(Index.BANK);
    loadIndex(Index.OTELLOGS);
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
  public void testCaseCanBePushedDownAsRangeQuery() throws IOException {
    // CASE 1: Range - Metric
    // 1.1 Range - Metric
    JSONObject actual1 =
        executeQuery(
            String.format(
                "source=%s | eval age_range = case(age < 30, 'u30', age < 40, 'u40' else 'u100') |"
                    + " stats avg(age) as avg_age by age_range",
                TEST_INDEX_BANK));
    verifySchema(actual1, schema("avg_age", "double"), schema("age_range", "string"));
    verifyDataRows(actual1, rows(28.0, "u30"), rows(35.0, "u40"));

    // 1.2 Range - Metric (COUNT)
    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s | eval age_range = case(age < 30, 'u30', age >= 30 and age < 40, 'u40'"
                    + " else 'u100') | stats avg(age) by age_range",
                TEST_INDEX_BANK));
    verifySchema(actual2, schema("avg(age)", "double"), schema("age_range", "string"));
    verifyDataRows(actual2, rows(28.0, "u30"), rows(35.0, "u40"));

    // 1.3 Range - Range - Metric
    JSONObject actual3 =
        executeQuery(
            String.format(
                "source=%s | eval age_range = case(age < 30, 'u30', age < 40, 'u40' else 'u100'),"
                    + " balance_range = case(balance < 20000, 'medium' else 'high') | stats"
                    + " avg(balance) as avg_balance by age_range, balance_range",
                TEST_INDEX_BANK));
    verifySchema(
        actual3,
        schema("avg_balance", "double"),
        schema("age_range", "string"),
        schema("balance_range", "string"));
    verifyDataRows(
        actual3,
        rows(32838.0, "u30", "high"),
        closeTo(8761.333333333334, "u40", "medium"),
        rows(42617.0, "u40", "high"));

    // 1.4 Range - Metric (With null & discontinuous ranges)
    JSONObject actual4 =
        executeQuery(
            String.format(
                "source=%s | eval age_range = case(age < 30, 'u30', (age >= 35 and age < 40) or age"
                    + " >= 80, '30-40 or >=80') | stats avg(balance) by age_range",
                TEST_INDEX_BANK));
    verifySchema(actual4, schema("avg(balance)", "double"), schema("age_range", "string"));
    // There's such a discrepancy because null cannot be the key for a range query
    if (isPushdownDisabled()) {
      verifyDataRows(
          actual4,
          rows(32838.0, "u30"),
          rows(30497.0, null),
          closeTo(20881.333333333332, "30-40 or >=80"));
    } else {
      verifyDataRows(
          actual4,
          rows(32838.0, "u30"),
          rows(30497.0, "null"),
          closeTo(20881.333333333332, "30-40 or >=80"));
    }

    // 1.5 Should not be pushed because the range is not closed-open
    JSONObject actual5 =
        executeQuery(
            String.format(
                "source=%s | eval age_range = case(age < 30, 'u30', age >= 30 and age <= 40, 'u40'"
                    + " else 'u100') | stats avg(age) as avg_age by age_range",
                TEST_INDEX_BANK));
    verifySchema(actual5, schema("avg_age", "double"), schema("age_range", "string"));
    verifyDataRows(actual5, rows(35.0, "u40"), rows(28.0, "u30"));
  }

  @Test
  public void testCaseCanBePushedDownAsCompositeRangeQuery() throws IOException {
    // CASE 2: Composite - Range - Metric
    // 2.1 Composite (term) - Range - Metric
    JSONObject actual6 =
        executeQuery(
            String.format(
                "source=%s | eval age_range = case(age < 30, 'u30' else 'a30') | stats avg(balance)"
                    + " by state, age_range",
                TEST_INDEX_BANK));
    verifySchema(
        actual6,
        schema("avg(balance)", "double"),
        schema("state", "string"),
        schema("age_range", "string"));
    verifyDataRows(
        actual6,
        rows(39225.0, "IL", "a30"),
        rows(48086.0, "IN", "a30"),
        rows(4180.0, "MD", "a30"),
        rows(40540.0, "PA", "a30"),
        rows(5686.0, "TN", "a30"),
        rows(32838.0, "VA", "u30"),
        rows(16418.0, "WA", "a30"));

    // 2.2 Composite (date histogram) - Range - Metric
    JSONObject actual7 =
        executeQuery(
            "source=opensearch-sql_test_index_time_data | eval value_range = case(value < 7000,"
                + " 'small' else 'large') | stats avg(value) by value_range, span(@timestamp,"
                + " 1month)");
    verifySchema(
        actual7,
        schema("avg(value)", "double"),
        schema("span(@timestamp,1month)", "timestamp"),
        schema("value_range", "string"));

    verifyDataRows(
        actual7,
        closeTo(6642.521739130435, "2025-07-01 00:00:00", "small"),
        closeTo(8381.917808219177, "2025-07-01 00:00:00", "large"),
        rows(6489.0, "2025-08-01 00:00:00", "small"),
        rows(8375.0, "2025-08-01 00:00:00", "large"));

    // 2.3 Composite(2 fields) - Range - Metric (with count)
    JSONObject actual8 =
        executeQuery(
            String.format(
                "source=%s | eval age_range = case(age < 30, 'u30' else 'a30') | stats"
                    + " avg(balance), count() by age_range, state, gender",
                TEST_INDEX_BANK));
    verifySchema(
        actual8,
        schema("avg(balance)", "double"),
        schema("count()", "bigint"),
        schema("age_range", "string"),
        schema("state", "string"),
        schema("gender", "string"));
    verifyDataRows(
        actual8,
        rows(5686.0, 1, "a30", "TN", "M"),
        rows(16418.0, 1, "a30", "WA", "M"),
        rows(40540.0, 1, "a30", "PA", "F"),
        rows(4180.0, 1, "a30", "MD", "M"),
        rows(32838.0, 1, "u30", "VA", "F"),
        rows(39225.0, 1, "a30", "IL", "M"),
        rows(48086.0, 1, "a30", "IN", "F"));

    // 2.4 Composite (2 fields) - Range - Range - Metric (with count)
    JSONObject actual9 =
        executeQuery(
            String.format(
                "source=%s | eval age_range = case(age < 35, 'u35' else 'a35'), balance_range ="
                    + " case(balance < 20000, 'medium' else 'high') | stats avg(balance) as"
                    + " avg_balance by age_range, balance_range, state",
                TEST_INDEX_BANK));
    verifySchema(
        actual9,
        schema("avg_balance", "double"),
        schema("age_range", "string"),
        schema("balance_range", "string"),
        schema("state", "string"));
    verifyDataRows(
        actual9,
        rows(39225.0, "u35", "high", "IL"),
        rows(48086.0, "u35", "high", "IN"),
        rows(4180.0, "u35", "medium", "MD"),
        rows(40540.0, "a35", "high", "PA"),
        rows(5686.0, "a35", "medium", "TN"),
        rows(32838.0, "u35", "high", "VA"),
        rows(16418.0, "a35", "medium", "WA"));

    // 2.5 Should not be pushed because case result expression is not constant
    JSONObject actual10 =
        executeQuery(
            String.format(
                "source=%s | eval age_range = case(age < 35, 'u35' else email) | stats avg(balance)"
                    + " as avg_balance by age_range, state",
                TEST_INDEX_BANK));
    verifySchema(
        actual10,
        schema("avg_balance", "double"),
        schema("age_range", "string"),
        schema("state", "string"));
    verifyDataRows(
        actual10,
        rows(32838.0, "u35", "VA"),
        rows(4180.0, "u35", "MD"),
        rows(48086.0, "u35", "IN"),
        rows(40540.0, "virginiaayala@filodyne.com", "PA"),
        rows(39225.0, "u35", "IL"),
        rows(5686.0, "hattiebond@netagy.com", "TN"),
        rows(16418.0, "elinorratliff@scentric.com", "WA"));
  }

  @Test
  public void testCaseAggWithNullValues() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s"
                    + "| eval age_category = case("
                    + "    age < 20, 'teenager',"
                    + "    age < 70, 'adult',"
                    + "    age >= 70, 'senior'"
                    + "    else 'unknown')"
                    + "| stats avg(age) by age_category",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));
    verifySchema(actual, schema("avg(age)", "double"), schema("age_category", "string"));
    // There is such discrepancy because range aggregations will ignore null values
    if (isPushdownDisabled()) {
      verifyDataRows(
          actual,
          rows(10, "teenager"),
          rows(25, "adult"),
          rows(70, "senior"),
          rows(null, "unknown"));
    } else {
      verifyDataRows(actual, rows(10, "teenager"), rows(25, "adult"), rows(70, "senior"));
    }
  }

  @Test
  public void testNestedCaseAggWithAutoDateHistogram() throws IOException {
    // TODO: Remove after resolving: https://github.com/opensearch-project/sql/issues/4578
    Assume.assumeFalse(
        "The query cannot be executed when pushdown is disabled due to implementation defects of"
            + " the bin command",
        isPushdownDisabled());
    JSONObject actual1 =
        executeQuery(
            String.format(
                "source=%s |  bin @timestamp bins=2 | eval severity_range = case(severityNumber <"
                    + " 16, 'minor' else 'severe') | stats avg(severityNumber), count() by"
                    + " @timestamp, severity_range, flags",
                TEST_INDEX_OTEL_LOGS));
    verifySchema(
        actual1,
        schema("avg(severityNumber)", "double"),
        schema("count()", "bigint"),
        schema("@timestamp", "timestamp"),
        schema("severity_range", "string"),
        schema("flags", "bigint"));

    verifyDataRows(
        actual1,
        rows(8.85, 20, "2024-01-15 10:30:02", "minor", 0),
        rows(20, 9, "2024-01-15 10:30:02", "severe", 0),
        rows(9, 1, "2024-01-15 10:30:00", "minor", 1),
        rows(17, 1, "2024-01-15 10:30:00", "severe", 1),
        rows(1, 1, "2024-01-15 10:30:05", "minor", 1));

    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s |  bin @timestamp bins=100 | eval severity_range = case(severityNumber <"
                    + " 16, 'minor' else 'severe') | stats avg(severityNumber), count() by"
                    + " @timestamp, severity_range, flags",
                TEST_INDEX_OTEL_LOGS));
    verifySchema(
        actual2,
        schema("avg(severityNumber)", "double"),
        schema("count()", "bigint"),
        schema("@timestamp", "timestamp"),
        schema("severity_range", "string"),
        schema("flags", "bigint"));
    verifyNumOfRows(actual2, 32);
  }
}
