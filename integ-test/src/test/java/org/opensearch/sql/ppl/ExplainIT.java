/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_OTEL_LOGS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WEBLOGS;
import static org.opensearch.sql.util.MatcherUtils.assertJsonEqualsIgnoreId;
import static org.opensearch.sql.util.MatcherUtils.assertYamlEqualsJsonIgnoreId;

import java.io.IOException;
import java.util.Locale;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.TestUtils;

public class ExplainIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK);
    loadIndex(Index.DATE_FORMATS);
    loadIndex(Index.WEBLOG);
    loadIndex(Index.OTELLOGS);
  }

  @Test
  public void testExplain() throws IOException {
    String expected = loadExpectedPlan("explain_output.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| where age > 30 "
                + "| stats avg(age) AS avg_age by state, city "
                + "| sort state "
                + "| fields - city "
                + "| eval age2 = avg_age + 2 "
                + "| dedup age2 "
                + "| fields age2"));
  }

  @Test
  public void testFilterPushDownExplain() throws IOException {
    String expected = loadExpectedPlan("explain_filter_push.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| where age > 30 "
                + "| where age < 40 "
                + "| where balance > 10000 "
                + "| fields age"));
  }

  @Test
  public void testFilterByCompareStringTimestampPushDownExplain() throws IOException {
    String expected = loadExpectedPlan("explain_filter_push_compare_timestamp_string.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_bank"
                + "| where birthdate > '2016-12-08 00:00:00.000000000' "
                + "| where birthdate < '2018-11-09 00:00:00.000000000' "));
  }

  @Test
  public void testFilterByCompareStringDatePushDownExplain() throws IOException {
    String expected = loadExpectedPlan("explain_filter_push_compare_date_string.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_date_formats | fields yyyy-MM-dd"
                + "| where yyyy-MM-dd > '2016-12-08 00:00:00.123456789' "
                + "| where yyyy-MM-dd < '2018-11-09 00:00:00.000000000' "));
  }

  @Test
  public void testFilterByCompareStringTimePushDownExplain() throws IOException {
    String expected = loadExpectedPlan("explain_filter_push_compare_time_string.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_date_formats | fields custom_time"
                + "| where custom_time > '2016-12-08 12:00:00.123456789' "
                + "| where custom_time < '2018-11-09 19:00:00.123456789' "));
  }

  @Test
  public void testFilterByCompareIPCoercion() throws IOException {
    // Should automatically cast the string literal to IP and pushdown it as a range query
    assertJsonEqualsIgnoreId(
        loadExpectedPlan("explain_filter_compare_ip.json"),
        explainQueryToString(
            String.format(
                Locale.ROOT,
                "source=%s | where host > '1.1.1.1' | fields host",
                TEST_INDEX_WEBLOGS)));
  }

  @Test
  public void testFilterByCompareIpv6Swapped() throws IOException {
    // Ignored in v2: the serialized string is unstable because of function properties
    Assume.assumeTrue(isCalciteEnabled());
    // Test swapping ip and string. In v2, this is pushed down as script;
    // with Calcite, it will still be pushed down as a range query
    assertJsonEqualsIgnoreId(
        loadExpectedPlan("explain_filter_compare_ipv6_swapped.json"),
        explainQueryToString(
            String.format(
                Locale.ROOT,
                "source=%s | where '::ffff:1234' <= host | fields host",
                TEST_INDEX_WEBLOGS)));
  }

  @Test
  public void testWeekArgumentCoercion() throws IOException {
    String expected = loadExpectedPlan("explain_week_argument_coercion.json");
    // Week accepts WEEK(timestamp/date/time, [optional int]), it should cast the string
    // argument to timestamp with Calcite. In v2, it accepts string, so there is no cast.
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                Locale.ROOT,
                "source=%s |  eval w = week('2024-12-10') | fields w",
                TEST_INDEX_ACCOUNT)));
  }

  @Test
  public void testFilterAndAggPushDownExplain() throws IOException {
    String expected = loadExpectedPlan("explain_filter_agg_push.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| where age > 30 "
                + "| stats avg(age) AS avg_age by state, city"));
  }

  @Test
  public void testCountAggPushDownExplain() throws IOException {
    String expected = loadExpectedPlan("explain_count_agg_push.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString("source=opensearch-sql_test_index_account | stats count() as cnt"));
  }

  @Test
  public void testSortPushDownExplain() throws IOException {
    String expected = loadExpectedPlan("explain_sort_push.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| sort age "
                + "| where age > 30"
                + "| fields age"));
  }

  @Test
  public void testSortWithCountPushDownExplain() throws IOException {
    String expected = loadExpectedPlan("explain_sort_count_push.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString("source=opensearch-sql_test_index_account | sort 5 age | fields age"));
  }

  @Test
  public void testSortWithDescPushDownExplain() throws IOException {
    String expected = loadExpectedPlan("explain_sort_desc_push.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | sort age, - firstname desc | fields age,"
                + " firstname"));
  }

  @Test
  public void testSortWithTypePushDownExplain() throws IOException {
    String expected = loadExpectedPlan("explain_sort_type_push.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | sort num(age) | fields age"));
  }

  @Test
  public void testSortWithAggregationExplain() throws IOException {
    // Sorts whose by fields are aggregators should not be pushed down
    String expected = loadExpectedPlan("explain_sort_agg_push.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| stats avg(age) AS avg_age by state, city "
                + "| sort avg_age"));

    // sorts whose by fields are not aggregators can be pushed down.
    // This test is covered in testExplain
  }

  @Test
  public void testMultiSortPushDownExplain() throws IOException {
    // TODO: Fix the expected output in expectedOutput/ppl/explain_multi_sort_push.json (v2)
    //  balance and gender should take precedence over account_number and firstname
    String expected = loadExpectedPlan("explain_multi_sort_push.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account "
                + "| sort account_number, firstname, address, balance "
                + "| sort - balance, - gender, account_number "
                + "| fields account_number, firstname, address, balance, gender"));
  }

  @Test
  public void testSortThenAggregatePushDownExplain() throws IOException {
    String expected = loadExpectedPlan("explain_sort_then_agg_push.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| sort balance, age "
                + "| stats avg(balance) by state"));
  }

  @Test
  public void testSortWithRenameExplain() throws IOException {
    String expected = loadExpectedPlan("explain_sort_rename_push.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account "
                + "| rename firstname as name "
                + "| eval alias = name "
                + "|  sort alias "
                + "| fields alias"));
  }

  /**
   * Pushdown SORT and LIMIT Sort should be pushed down since DSL process sort before limit when
   * they coexist
   */
  @Test
  public void testSortThenLimitExplain() throws IOException {
    String expected = loadExpectedPlan("explain_sort_then_limit_push.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| sort age "
                + "| head 5 "
                + "| fields age"));
  }

  /**
   * Push down LIMIT only Sort should NOT be pushed down since DSL process limit before sort when
   * they coexist
   */
  @Test
  public void testLimitThenSortExplain() throws IOException {
    // TODO: Fix the expected output in expectedOutput/ppl/explain_limit_then_sort_push.json (v2)
    //  limit-then-sort should not be pushed down.
    String expected = loadExpectedPlan("explain_limit_then_sort_push.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 5 "
                + "| sort age "
                + "| fields age"));
  }

  @Test
  public void testLimitPushDownExplain() throws IOException {
    String expected = loadExpectedPlan("explain_limit_push.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| eval ageMinus = age - 30 "
                + "| head 5 "
                + "| fields ageMinus"));
  }

  @Test
  public void testLimitWithFilterPushdownExplain() throws IOException {
    String expectedFilterThenLimit = loadExpectedPlan("explain_filter_then_limit_push.yaml");
    assertYamlEqualsJsonIgnoreId(
        expectedFilterThenLimit,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| where age > 30 "
                + "| head 5 "
                + "| fields age"));

    // The filter in limit-then-filter queries should not be pushed since the current DSL will
    // execute it as filter-then-limit
    String expectedLimitThenFilter = loadExpectedPlan("explain_limit_then_filter_push.yaml");
    assertYamlEqualsJsonIgnoreId(
        expectedLimitThenFilter,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 5 "
                + "| where age > 30 "
                + "| fields age"));
  }

  @Test
  public void testMultipleLimitExplain() throws IOException {
    String expected5Then10 = loadExpectedPlan("explain_limit_5_10_push.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected5Then10,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 5 "
                + "| head 10 "
                + "| fields age"));

    String expected10Then5 = loadExpectedPlan("explain_limit_10_5_push.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected10Then5,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 10 "
                + "| head 5 "
                + "| fields age"));

    String expected10from1then10from2 = loadExpectedPlan("explain_limit_10from1_10from2_push.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected10from1then10from2,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 10 from 1 "
                + "| head 10 from 2 "
                + "| fields age"));

    // The second limit should not be pushed down for limit-filter-limit queries
    String expected10ThenFilterThen5 = loadExpectedPlan("explain_limit_10_filter_5_push.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected10ThenFilterThen5,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 10 "
                + "| where age > 30 "
                + "| head 5 "
                + "| fields age"));
  }

  @Test
  public void testLimitWithMultipleOffsetPushdownExplain() throws IOException {
    String expected = loadExpectedPlan("explain_limit_offsets_push.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 10 from 1 "
                + "| head 5 from 2 "
                + "| fields age"));
  }

  @Test
  public void testFillNullPushDownExplain() throws IOException {
    String expected = loadExpectedPlan("explain_fillnull_push.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + " | fillnull with -1 in age,balance | fields age, balance"));
  }

  @Test
  public void testTrendlinePushDownExplain() throws IOException {
    String expected = loadExpectedPlan("explain_trendline_push.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 5 "
                + "| trendline sma(2, age) as ageTrend "
                + "| fields ageTrend"));
  }

  @Test
  public void testTrendlineWithSortPushDownExplain() throws IOException {
    String expected = loadExpectedPlan("explain_trendline_sort_push.yaml");
    // Sort will not be pushed down because there's a head before it.
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 5 "
                + "| trendline sort age sma(2, age) as ageTrend "
                + "| fields ageTrend"));
  }

  @Test
  public void testExplainModeUnsupportedInV2() throws IOException {
    try {
      executeQueryToString(
          "explain cost source=opensearch-sql_test_index_account | where age = 20 | fields name,"
              + " city");
    } catch (ResponseException e) {
      final String entity = TestUtils.getResponseBody(e.getResponse());
      assertThat(entity, containsString("Explain mode COST is not supported in v2 engine"));
    }
  }

  @Test
  public void testPatternsSimplePatternMethodWithoutAggExplain() throws IOException {
    // TODO: Correct calcite expected result once pushdown is supported
    String expected = loadExpectedPlan("explain_patterns_simple_pattern.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString("source=opensearch-sql_test_index_account | patterns email"));
  }

  @Test
  public void testPatternsSimplePatternMethodWithAggPushDownExplain() throws IOException {
    String expected = loadExpectedPlan("explain_patterns_simple_pattern_agg_push.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | patterns email mode=aggregation"));
  }

  @Test
  public void testPatternsBrainMethodWithAggPushDownExplain() throws IOException {
    // TODO: Correct calcite expected result once pushdown is supported
    String expected = loadExpectedPlan("explain_patterns_brain_agg_push.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| patterns email method=brain mode=aggregation"));
  }

  @Test
  public void testStatsBySpan() throws IOException {
    String expected = loadExpectedPlan("explain_stats_by_span.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format("source=%s | stats count() by span(age,10)", TEST_INDEX_BANK)));
  }

  @Test
  public void testStatsBySpanNonBucketNullable() throws IOException {
    String expected = loadExpectedPlan("explain_stats_by_span_non_bucket_nullable.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | stats bucket_nullable=false count() by span(age,10)",
                TEST_INDEX_BANK)));
  }

  @Test
  public void testStatsByTimeSpan() throws IOException {
    String expected = loadExpectedPlan("explain_stats_by_timespan.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format("source=%s | stats count() by span(birthdate,1m)", TEST_INDEX_BANK)));

    expected = loadExpectedPlan("explain_stats_by_timespan2.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format("source=%s | stats count() by span(birthdate,1M)", TEST_INDEX_BANK)));

    // bucket_nullable doesn't impact by-span-time
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | stats bucket_nullable=false count() by span(birthdate,1M)",
                TEST_INDEX_BANK)));
  }

  @Ignore("https://github.com/opensearch-project/OpenSearch/issues/3725")
  public void testDedupPushdown() throws IOException {
    String expected = loadExpectedPlan("explain_dedup_push.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | fields account_number, gender, age"
                + " | dedup 1 gender"));
  }

  @Test
  public void testDedupKeepEmptyTruePushdown() throws IOException {
    String expected = loadExpectedPlan("explain_dedup_keepempty_true_push.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | fields account_number, gender, age"
                + " | dedup gender KEEPEMPTY=true"));
  }

  @Ignore("https://github.com/opensearch-project/OpenSearch/issues/3725")
  public void testDedupKeepEmptyFalsePushdown() throws IOException {
    String expected = loadExpectedPlan("explain_dedup_keepempty_false_push.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | fields account_number, gender, age"
                + " | dedup gender KEEPEMPTY=false"));
  }

  @Test
  public void testSingleFieldRelevanceQueryFunctionExplain() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_single_field_relevance_push.json")
            : loadFromFile("expectedOutput/ppl/explain_single_field_relevance_push.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| where match(email, '*@gmail.com', boost=1.0)"));
  }

  @Test
  public void testMultiFieldsRelevanceQueryFunctionExplain() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_multi_fields_relevance_push.json")
            : loadFromFile("expectedOutput/ppl/explain_multi_fields_relevance_push.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| where simple_query_string(['email', name 4.0], 'gmail',"
                + " default_operator='or', analyzer=english)"));
  }

  @Test
  public void testKeywordLikeFunctionExplain() throws IOException {
    String expected = loadExpectedPlan("explain_keyword_like_function.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | where like(firstname, '%mbe%')"));
  }

  @Test
  public void testTextLikeFunctionExplain() throws IOException {
    String expected = loadExpectedPlan("explain_text_like_function.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | where like(address, '%Holmes%')"));
  }

  @Ignore("The serialized string is unstable because of function properties")
  @Test
  public void testFilterScriptPushDownExplain() throws Exception {
    String expected = loadExpectedPlan("explain_filter_script_push.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | where firstname ='Amber' and age - 2 = 30 |"
                + " fields firstname, age"));
  }

  @Ignore("The serialized string is unstable because of function properties")
  @Test
  public void testFilterFunctionScriptPushDownExplain() throws Exception {
    String expected = loadExpectedPlan("explain_filter_function_script_push.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account |  where length(firstname) = 5 and abs(age) ="
                + " 32 and balance = 39225 | fields firstname, age"));
  }

  @Test
  public void testDifferentFilterScriptPushDownBehaviorExplain() throws Exception {
    String explainedPlan =
        explainQueryToString(
            "source=opensearch-sql_test_index_account |  where firstname != '' | fields firstname");
    if (isCalciteEnabled()) {
      // Calcite pushdown as pure filter query
      String expected = loadExpectedPlan("explain_filter_script_push_diff.json");
      assertJsonEqualsIgnoreId(expected, explainedPlan);
    } else {
      // V2 pushdown as script
      assertTrue(explainedPlan.contains("{\\\"script\\\":"));
    }
  }

  @Test
  public void testExplainOnTake() throws IOException {
    String expected = loadExpectedPlan("explain_take.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats take(firstname, 2) as take"));
  }

  @Test
  public void testExplainOnPercentile() throws IOException {
    String expected = loadExpectedPlan("explain_percentile.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats percentile(balance, 50) as p50,"
                + " percentile(balance, 90) as p90"));
  }

  @Test
  public void testExplainOnAggregationWithFunction() throws IOException {
    String expected = loadExpectedPlan("explain_agg_with_script.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | eval len = length(gender) | stats sum(balance + 100) as sum by len,"
                    + " gender ",
                TEST_INDEX_BANK)));
  }

  protected String loadExpectedPlan(String fileName) throws IOException {
    String prefix;
    if (isCalciteEnabled()) {
      if (isPushdownDisabled()) {
        prefix = "expectedOutput/calcite_no_pushdown/";
      } else {
        prefix = "expectedOutput/calcite/";
      }
    } else {
      prefix = "expectedOutput/ppl/";
    }
    return loadFromFile(prefix + fileName);
  }

  // Search command explain examples - 3 core use cases

  @Test
  public void testExplainSearchBasicText() throws IOException {
    // Example 1: Basic text search without field specification
    String expected = loadExpectedPlan("explain_search_basic_text.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(String.format("search source=%s ERROR", TEST_INDEX_OTEL_LOGS)));
  }

  @Test
  public void testExplainSearchNumericComparison() throws IOException {
    // Example 2: Numeric field comparison with greater than
    String expected = loadExpectedPlan("explain_search_numeric_comparison.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format("search source=%s severityNumber>15", TEST_INDEX_OTEL_LOGS)));
  }

  @Test
  public void testExplainSearchWildcardStar() throws IOException {
    // Example 3: Wildcard search with asterisk for pattern matching
    String expected = loadExpectedPlan("explain_search_wildcard_star.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format("search source=%s severityText=ERR*", TEST_INDEX_OTEL_LOGS)));
  }
}
