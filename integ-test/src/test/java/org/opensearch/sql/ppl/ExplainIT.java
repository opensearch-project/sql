/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.assertJsonEqualsIgnoreId;

import java.io.IOException;
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
  }

  @Test
  public void testExplain() throws IOException {
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_output.json")
            : loadFromFile("expectedOutput/ppl/explain_output.json");
    assertJsonEqualsIgnoreId(
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
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_filter_push.json")
            : loadFromFile("expectedOutput/ppl/explain_filter_push.json");

    assertJsonEqualsIgnoreId(
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
    String expected =
        isCalciteEnabled()
            ? loadFromFile(
                "expectedOutput/calcite/explain_filter_push_compare_timestamp_string.json")
            : loadFromFile("expectedOutput/ppl/explain_filter_push_compare_timestamp_string.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_bank"
                + "| where birthdate > '2016-12-08 00:00:00.000000000' "
                + "| where birthdate < '2018-11-09 00:00:00.000000000' "));
  }

  @Test
  public void testFilterByCompareStringDatePushDownExplain() throws IOException {
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_filter_push_compare_date_string.json")
            : loadFromFile("expectedOutput/ppl/explain_filter_push_compare_date_string.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_date_formats | fields yyyy-MM-dd"
                + "| where yyyy-MM-dd > '2016-12-08 00:00:00.123456789' "
                + "| where yyyy-MM-dd < '2018-11-09 00:00:00.000000000' "));
  }

  @Test
  public void testFilterByCompareStringTimePushDownExplain() throws IOException {
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_filter_push_compare_time_string.json")
            : loadFromFile("expectedOutput/ppl/explain_filter_push_compare_time_string.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_date_formats | fields custom_time"
                + "| where custom_time > '2016-12-08 12:00:00.123456789' "
                + "| where custom_time < '2018-11-09 19:00:00.123456789' "));
  }

  @Test
  public void testFilterAndAggPushDownExplain() throws IOException {
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_filter_agg_push.json")
            : loadFromFile("expectedOutput/ppl/explain_filter_agg_push.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| where age > 30 "
                + "| stats avg(age) AS avg_age by state, city"));
  }

  @Test
  public void testSortPushDownExplain() throws IOException {
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_sort_push.json")
            : loadFromFile("expectedOutput/ppl/explain_sort_push.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| sort age "
                + "| where age > 30"
                + "| fields age"));
  }

  @Test
  public void testSortWithAggregationExplain() throws IOException {
    // Sorts whose by fields are aggregators should not be pushed down
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_sort_agg_push.json")
            : loadFromFile("expectedOutput/ppl/explain_sort_agg_push.json");

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
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_multi_sort_push.json")
            : loadFromFile("expectedOutput/ppl/explain_multi_sort_push.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account "
                + "| sort account_number, firstname, address, balance "
                + "| sort - balance, - gender, address "
                + "| fields account_number, firstname, address, balance, gender"));
  }

  @Test
  public void testSortThenAggregatePushDownExplain() throws IOException {
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_sort_then_agg_push.json")
            : loadFromFile("expectedOutput/ppl/explain_sort_then_agg_push.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| sort balance, age "
                + "| stats avg(balance) by state"));
  }

  @Test
  public void testSortWithRenameExplain() throws IOException {
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_sort_rename_push.json")
            : loadFromFile("expectedOutput/ppl/explain_sort_rename_push.json");

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
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_sort_then_limit_push.json")
            : loadFromFile("expectedOutput/ppl/explain_sort_then_limit_push.json");
    assertJsonEqualsIgnoreId(
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
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_limit_then_sort_push.json")
            : loadFromFile("expectedOutput/ppl/explain_limit_then_sort_push.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 5 "
                + "| sort age "
                + "| fields age"));
  }

  @Test
  public void testLimitPushDownExplain() throws IOException {
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_limit_push.json")
            : loadFromFile("expectedOutput/ppl/explain_limit_push.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| eval ageMinus = age - 30 "
                + "| head 5 "
                + "| fields ageMinus"));
  }

  @Test
  public void testLimitWithFilterPushdownExplain() throws IOException {
    String expectedFilterThenLimit =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_filter_then_limit_push.json")
            : loadFromFile("expectedOutput/ppl/explain_filter_then_limit_push.json");
    assertJsonEqualsIgnoreId(
        expectedFilterThenLimit,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| where age > 30 "
                + "| head 5 "
                + "| fields age"));

    // The filter in limit-then-filter queries should not be pushed since the current DSL will
    // execute it as filter-then-limit
    String expectedLimitThenFilter =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_limit_then_filter_push.json")
            : loadFromFile("expectedOutput/ppl/explain_limit_then_filter_push.json");
    assertJsonEqualsIgnoreId(
        expectedLimitThenFilter,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 5 "
                + "| where age > 30 "
                + "| fields age"));
  }

  @Test
  public void testMultipleLimitExplain() throws IOException {
    String expected5Then10 =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_limit_5_10_push.json")
            : loadFromFile("expectedOutput/ppl/explain_limit_5_10_push.json");
    assertJsonEqualsIgnoreId(
        expected5Then10,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 5 "
                + "| head 10 "
                + "| fields age"));

    String expected10Then5 =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_limit_10_5_push.json")
            : loadFromFile("expectedOutput/ppl/explain_limit_10_5_push.json");
    assertJsonEqualsIgnoreId(
        expected10Then5,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 10 "
                + "| head 5 "
                + "| fields age"));

    String expected10from1then10from2 =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_limit_10from1_10from2_push.json")
            : loadFromFile("expectedOutput/ppl/explain_limit_10from1_10from2_push.json");
    assertJsonEqualsIgnoreId(
        expected10from1then10from2,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 10 from 1 "
                + "| head 10 from 2 "
                + "| fields age"));

    // The second limit should not be pushed down for limit-filter-limit queries
    String expected10ThenFilterThen5 =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_limit_10_filter_5_push.json")
            : loadFromFile("expectedOutput/ppl/explain_limit_10_filter_5_push.json");
    assertJsonEqualsIgnoreId(
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
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_limit_offsets_push.json")
            : loadFromFile("expectedOutput/ppl/explain_limit_offsets_push.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 10 from 1 "
                + "| head 5 from 2 "
                + "| fields age"));
  }

  @Test
  public void testFillNullPushDownExplain() throws IOException {
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_fillnull_push.json")
            : loadFromFile("expectedOutput/ppl/explain_fillnull_push.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + " | fillnull with -1 in age,balance | fields age, balance"));
  }

  @Test
  public void testTrendlinePushDownExplain() throws IOException {
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_trendline_push.json")
            : loadFromFile("expectedOutput/ppl/explain_trendline_push.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 5 "
                + "| trendline sma(2, age) as ageTrend "
                + "| fields ageTrend"));
  }

  @Test
  public void testTrendlineWithSortPushDownExplain() throws IOException {
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_trendline_sort_push.json")
            : loadFromFile("expectedOutput/ppl/explain_trendline_sort_push.json");

    // Sort will not be pushed down because there's a head before it.
    assertJsonEqualsIgnoreId(
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
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_patterns_simple_pattern.json")
            : loadFromFile("expectedOutput/ppl/explain_patterns_simple_pattern.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString("source=opensearch-sql_test_index_account | patterns email"));
  }

  @Test
  public void testPatternsSimplePatternMethodWithAggPushDownExplain() throws IOException {
    // TODO: Correct calcite expected result once pushdown is supported
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_patterns_simple_pattern_agg_push.json")
            : loadFromFile("expectedOutput/ppl/explain_patterns_simple_pattern_agg_push.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account" + "| patterns email mode=aggregation"));
  }

  @Test
  public void testPatternsBrainMethodWithAggPushDownExplain() throws IOException {
    // TODO: Correct calcite expected result once pushdown is supported
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_patterns_brain_agg_push.json")
            : loadFromFile("expectedOutput/ppl/explain_patterns_brain_agg_push.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| patterns email method=brain mode=aggregation"));
  }

  @Test
  public void testStatsBySpan() throws IOException {
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_stats_by_span.json")
            : loadFromFile("expectedOutput/ppl/explain_stats_by_span.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format("source=%s | stats count() by span(age,10)", TEST_INDEX_BANK)));
  }

  @Test
  public void testStatsByTimeSpan() throws IOException {
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_stats_by_timespan.json")
            : loadFromFile("expectedOutput/ppl/explain_stats_by_timespan.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format("source=%s | stats count() by span(birthdate,1m)", TEST_INDEX_BANK)));

    expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_stats_by_timespan2.json")
            : loadFromFile("expectedOutput/ppl/explain_stats_by_timespan2.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format("source=%s | stats count() by span(birthdate,1M)", TEST_INDEX_BANK)));
  }

  @Test
  public void testSingleFieldRelevanceQueryFunctionExplain() throws IOException {
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

  @Ignore("The serialized string is unstable because of function properties")
  @Test
  public void testFilterScriptPushDownExplain() throws Exception {
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_filter_script_push.json")
            : loadFromFile("expectedOutput/ppl/explain_filter_script_push.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | where firstname ='Amber' and age - 2 = 30 |"
                + " fields firstname, age"));
  }

  @Ignore("The serialized string is unstable because of function properties")
  @Test
  public void testFilterFunctionScriptPushDownExplain() throws Exception {
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_filter_function_script_push.json")
            : loadFromFile("expectedOutput/ppl/explain_filter_function_script_push.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account |  where length(firstname) = 5 and abs(age) ="
                + " 32 and balance = 39225 | fields firstname, age"));
  }
}
