/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_LOGS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_SIMPLE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STRINGS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WEBLOGS;
import static org.opensearch.sql.util.MatcherUtils.assertJsonEqualsIgnoreId;
import static org.opensearch.sql.util.MatcherUtils.assertYamlEqualsJsonIgnoreId;

import java.io.IOException;
import java.util.Locale;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.ppl.ExplainIT;

public class CalciteExplainIT extends ExplainIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.BANK_WITH_STRING_VALUES);
    loadIndex(Index.NESTED_SIMPLE);
    loadIndex(Index.TIME_TEST_DATA);
    loadIndex(Index.EVENTS);
    loadIndex(Index.LOGS);
  }

  @Override
  @Ignore("test only in v2")
  public void testExplainModeUnsupportedInV2() throws IOException {}

  // Only for Calcite
  @Test
  public void supportSearchSargPushDown_singleRange() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | where age >= 1.0 and age < 10 | fields age";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_sarg_filter_push_single_range.yaml");
    assertYamlEqualsJsonIgnoreId(expected, result);
  }

  // Only for Calcite
  @Test
  public void supportSearchSargPushDown_multiRange() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | where (age > 20 and age < 28) or (age > 25 and"
            + " age < 30) or (age >= 1 and age <= 10) or age = 0  | fields age";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_sarg_filter_push_multi_range.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  // Only for Calcite
  @Test
  public void supportSearchSargPushDown_timeRange() throws IOException {
    String query =
        "source=opensearch-sql_test_index_bank"
            + "| where birthdate >= '2016-12-08 00:00:00.000000000' "
            + "and birthdate < '2018-11-09 00:00:00.000000000'";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_sarg_filter_push_time_range.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  // Only for Calcite
  @Ignore("https://github.com/opensearch-project/OpenSearch/issues/3725")
  public void testJoinWithCriteriaAndMaxOption() throws IOException {
    String query =
        "source=opensearch-sql_test_index_bank | join max=1 left=l right=r on"
            + " l.account_number=r.account_number opensearch-sql_test_index_bank";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_join_with_criteria_max_option.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  // Only for Calcite
  @Ignore("https://github.com/opensearch-project/OpenSearch/issues/3725")
  public void testJoinWithFieldListAndMaxOption() throws IOException {
    String query =
        "source=opensearch-sql_test_index_bank | join type=inner max=1 account_number"
            + " opensearch-sql_test_index_bank";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_join_with_fields_max_option.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  // Only for Calcite
  @Test
  public void testJoinWithFieldList() throws IOException {
    String query =
        "source=opensearch-sql_test_index_bank | join type=outer account_number"
            + " opensearch-sql_test_index_bank";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_join_with_fields.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  // Only for Calcite
  @Test
  public void supportPushDownSortMergeJoin() throws IOException {
    String query =
        "source=opensearch-sql_test_index_bank| join left=l right=r on"
            + " l.account_number=r.account_number opensearch-sql_test_index_bank";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_merge_join_sort_push.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  // Only for Calcite
  @Ignore("We've supported script push down on text field")
  @Test
  public void supportPartialPushDown() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    // field `address` is text type without keyword subfield, so we cannot push it down.
    String query =
        "source=opensearch-sql_test_index_account | where (state = 'Seattle' or age < 10) and (age"
            + " >= 1 and address = '880 Holmes Lane') | fields age, address";
    var result = explainQueryToString(query);
    String expected = loadFromFile("expectedOutput/calcite/explain_partial_filter_push.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  // Only for Calcite
  @Ignore("We've supported script push down on text field")
  @Test
  public void supportPartialPushDown_NoPushIfAllFailed() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    // field `address` is text type without keyword subfield, so we cannot push it down.
    String query =
        "source=opensearch-sql_test_index_account | where (address = '671 Bristol Street' or age <"
            + " 10) and (age >= 10 or address = '880 Holmes Lane') | fields age, address";
    var result = explainQueryToString(query);
    String expected = loadFromFile("expectedOutput/calcite/explain_partial_filter_push2.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  // Only for Calcite
  @Test
  public void testExplainIsEmpty() throws IOException {
    // script pushdown
    String expected = loadExpectedPlan("explain_isempty.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | where isempty(firstname)"));
  }

  // Only for Calcite
  @Test
  public void testExplainIsBlank() throws IOException {
    // script pushdown
    String expected = loadExpectedPlan("explain_isblank.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | where isblank(firstname)"));
  }

  // Only for Calcite
  @Test
  public void testExplainIsEmptyOrOthers() throws IOException {
    // script pushdown
    String expected = loadExpectedPlan("explain_isempty_or_others.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | where gender = 'M' or isempty(firstname) or"
                + " isnull(firstname)"));
  }

  // Only for Calcite
  @Test
  public void testExplainIsNullOrOthers() throws IOException {
    // pushdown should work
    String expected = loadExpectedPlan("explain_isnull_or_others.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | where isnull(firstname) or gender = 'M'"));
  }

  @Ignore("We've supported script push down on text field")
  @Test
  public void supportPartialPushDownScript() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    // field `address` is text type without keyword subfield, so we cannot push it down.
    // But the second condition can be translated to script, so the second one is pushed down.
    String query =
        "source=opensearch-sql_test_index_account | where address = '671 Bristol Street' and age -"
            + " 2 = 30 | fields firstname, age, address";
    var result = explainQueryToString(query);
    String expected =
        loadFromFile("expectedOutput/calcite/explain_partial_filter_script_push.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  @Test
  public void testPartialPushdownFilterWithIsNull() throws IOException {
    // isnull(nested_field) should not be pushed down since DSL doesn't handle it correctly, but
    // name='david' can be pushed down
    String query =
        String.format(
            Locale.ROOT,
            "source=%s | where isnull(address) and name='david'",
            TEST_INDEX_NESTED_SIMPLE);
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_partial_filter_isnull.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  @Test
  public void testSkipScriptEncodingOnExtendedFormat() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String query =
        "source=opensearch-sql_test_index_account | where address = '671 Bristol Street' and age -"
            + " 2 = 30 | fields firstname, age, address";
    var result = explainQueryToString(query, true);
    String expected = loadFromFile("expectedOutput/calcite/explain_skip_script_encoding.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  // Only for Calcite, as v2 gets unstable serialized string for function
  @Test
  public void testFilterScriptPushDownExplain() throws Exception {
    super.testFilterScriptPushDownExplain();
  }

  // Only for Calcite, as v2 gets unstable serialized string for function
  @Test
  public void testFilterFunctionScriptPushDownExplain() throws Exception {
    super.testFilterFunctionScriptPushDownExplain();
  }

  @Test
  public void testFilterWithSearchCall() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_filter_with_search.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | where birthdate >= '2023-01-01 00:00:00' and birthdate < '2023-01-03"
                    + " 00:00:00' | stats count() by span(birthdate, 1d)",
                TEST_INDEX_BANK)));
  }

  @Test
  public void testExplainWithReverse() throws IOException {
    String result =
        executeWithReplace(
            "explain source=opensearch-sql_test_index_account | sort age | reverse | head 5");

    // Verify that the plan contains a LogicalSort with fetch (from head 5)
    assertTrue(result.contains("LogicalSort") && result.contains("fetch=[5]"));

    // Verify that reverse added a ROW_NUMBER and another sort (descending)
    assertTrue(result.contains("ROW_NUMBER()"));
    assertTrue(result.contains("dir0=[DESC]"));
  }

  @Test
  public void testExplainWithTimechartAvg() throws IOException {
    var result = explainQueryToString("source=events | timechart span=1m avg(cpu_usage) by host");
    String expected =
        !isPushdownDisabled()
            ? loadFromFile("expectedOutput/calcite/explain_timechart.yaml")
            : loadFromFile("expectedOutput/calcite/explain_timechart_no_pushdown.yaml");
    assertYamlEqualsJsonIgnoreId(expected, result);
  }

  @Test
  public void testExplainWithTimechartCount() throws IOException {
    var result = explainQueryToString("source=events | timechart span=1m count() by host");
    String expected =
        !isPushdownDisabled()
            ? loadFromFile("expectedOutput/calcite/explain_timechart_count.yaml")
            : loadFromFile("expectedOutput/calcite/explain_timechart_count_no_pushdown.yaml");
    assertYamlEqualsJsonIgnoreId(expected, result);
  }

  @Test
  public void noPushDownForAggOnWindow() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String query =
        "source=opensearch-sql_test_index_account | patterns address method=BRAIN  | stats count()"
            + " by patterns_field";
    var result = explainQueryToString(query);
    String expected = loadFromFile("expectedOutput/calcite/explain_agg_on_window.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  // Only for Calcite
  @Test
  public void supportPushDownScriptOnTextField() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String result =
        explainQueryToString(
            "explain source=opensearch-sql_test_index_account | where length(address) > 0 | eval"
                + " address_length = length(address) | stats count() by address_length");
    String expected = loadFromFile("expectedOutput/calcite/explain_script_push_on_text.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  @Test
  public void testExplainBinWithBins() throws IOException {
    String expected = loadExpectedPlan("explain_bin_bins.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString("source=opensearch-sql_test_index_account | bin age bins=3 | head 5"));
  }

  @Test
  public void testExplainStatsWithBinsOnTimeField() throws IOException {
    // TODO:  Remove this after addressing https://github.com/opensearch-project/sql/issues/4317
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_stats_bins_on_time.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=events | bin @timestamp bins=3 | stats count() by @timestamp"));

    expected = loadExpectedPlan("explain_stats_bins_on_time2.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=events | bin @timestamp bins=3 | stats avg(cpu_usage) by @timestamp"));
  }

  @Test
  public void testExplainBinWithSpan() throws IOException {
    String expected = loadExpectedPlan("explain_bin_span.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | bin age span=10 | head 5"));
  }

  @Test
  public void testExplainBinWithMinspan() throws IOException {
    String expected = loadExpectedPlan("explain_bin_minspan.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | bin age minspan=5 | head 5"));
  }

  @Test
  public void testExplainBinWithStartEnd() throws IOException {
    String expected = loadExpectedPlan("explain_bin_start_end.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | bin balance start=0 end=100001 | head 5"));
  }

  @Test
  public void testExplainBinWithAligntime() throws IOException {
    String expected = loadExpectedPlan("explain_bin_aligntime.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_time_data | bin @timestamp span=2h aligntime=latest |"
                + " head 5"));
  }

  @Test
  public void testExplainCountEval() throws IOException {
    String query =
        "source=opensearch-sql_test_index_bank | stats count(eval(age > 30)) as mature_count";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_count_eval_push.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  @Test
  public void testExplainCountEvalComplex() throws IOException {
    String query =
        "source=opensearch-sql_test_index_bank | stats count(eval(age > 30 and age < 50)) as"
            + " mature_count";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_count_eval_complex_push.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  @Test
  public void testEventstatsDistinctCountExplain() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String query =
        "source=opensearch-sql_test_index_account | eventstats dc(state) as distinct_states";
    var result = explainQueryToString(query);
    String expected = loadFromFile("expectedOutput/calcite/explain_eventstats_dc.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  @Test
  public void testEventstatsDistinctCountFunctionExplain() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String query =
        "source=opensearch-sql_test_index_account | eventstats distinct_count(state) as"
            + " distinct_states by gender";
    var result = explainQueryToString(query);
    String expected = loadFromFile("expectedOutput/calcite/explain_eventstats_distinct_count.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  // Only for Calcite, as v2 gets unstable serialized string for function
  @Test
  public void testExplainOnAggregationWithSumEnhancement() throws IOException {
    String expected = loadExpectedPlan("explain_agg_with_sum_enhancement.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | stats sum(balance), sum(balance + 100), sum(balance - 100),"
                    + " sum(balance * 100), sum(balance / 100) by gender",
                TEST_INDEX_BANK)));
  }

  @Test
  public void testExplainRegexMatchInWhereWithScriptPushdown() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String query =
        String.format("source=%s | where regex_match(name, 'hello')", TEST_INDEX_STRINGS);
    var result = explainQueryToString(query);
    String expected = loadFromFile("expectedOutput/calcite/explain_regex_match_in_where.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  @Test
  public void testExplainRegexMatchInEvalWithOutScriptPushdown() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String query =
        String.format(
            "source=%s |eval has_hello = regex_match(name, 'hello') | fields has_hello",
            TEST_INDEX_STRINGS);
    var result = explainQueryToString(query);
    String expected = loadFromFile("expectedOutput/calcite/explain_regex_match_in_eval.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  // Only for Calcite
  @Test
  public void testExplainOnEarliestLatest() throws IOException {
    String expected = loadExpectedPlan("explain_earliest_latest.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | stats earliest(message) as earliest_message, latest(message) as"
                    + " latest_message by server",
                TEST_INDEX_LOGS)));
  }

  // Only for Calcite
  @Test
  public void testExplainOnEarliestLatestWithCustomTimeField() throws IOException {
    String expected = loadExpectedPlan("explain_earliest_latest_custom_time.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | stats earliest(message, created_at) as earliest_message,"
                    + " latest(message, created_at) as latest_message by level",
                TEST_INDEX_LOGS)));
  }

  // Only for Calcite
  @Test
  public void testExplainOnFirstLast() throws IOException {
    String expected = loadExpectedPlan("explain_first_last.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | stats first(firstname) as first_name, last(firstname) as"
                    + " last_name by gender",
                TEST_INDEX_BANK)));
  }

  // Only for Calcite
  public void testExplainOnEventstatsEarliestLatest() throws IOException {
    String expected = loadExpectedPlan("explain_eventstats_earliest_latest.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | eventstats earliest(message) as earliest_message, latest(message) as"
                    + " latest_message by server",
                TEST_INDEX_LOGS)));
  }

  // Only for Calcite
  @Test
  public void testExplainOnEventstatsEarliestLatestWithCustomTimeField() throws IOException {
    String expected = loadExpectedPlan("explain_eventstats_earliest_latest_custom_time.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | eventstats earliest(message, created_at) as earliest_message,"
                    + " latest(message, created_at) as latest_message by level",
                TEST_INDEX_LOGS)));
  }

  // Only for Calcite
  @Test
  public void testExplainOnEventstatsEarliestLatestNoGroupBy() throws IOException {
    String expected = loadExpectedPlan("explain_eventstats_earliest_latest_no_group.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | eventstats earliest(message) as earliest_message, latest(message) as"
                    + " latest_message",
                TEST_INDEX_LOGS)));
  }

  @Test
  public void testListAggregationExplain() throws IOException {
    String expected = loadExpectedPlan("explain_list_aggregation.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats list(age) as age_list"));
  }

  @Test
  public void testValuesAggregationExplain() throws IOException {
    String expected = loadExpectedPlan("explain_values_aggregation.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats values(age) as age_values"));
  }

  @Test
  public void testRegexExplain() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | regex lastname='^[A-Z][a-z]+$' | head 5";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_regex.yaml");
    assertYamlEqualsJsonIgnoreId(expected, result);
  }

  @Test
  public void testRegexNegatedExplain() throws IOException {
    String query = "source=opensearch-sql_test_index_account | regex lastname!='.*son$' | head 5";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_regex_negated.yaml");
    assertYamlEqualsJsonIgnoreId(expected, result);
  }

  @Test
  public void testSimpleSortExpressionPushDownExplain() throws Exception {
    String query =
        "source=opensearch-sql_test_index_bank| eval age2 = age + 2 | sort age2 | fields age, age2";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_simple_sort_expr_push.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  @Test
  public void testSimpleSortExpressionPushDownWithOnlyExprProjected() throws Exception {
    String query =
        "source=opensearch-sql_test_index_bank| eval b = balance + 1 | sort b | fields b";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_simple_sort_expr_single_expr_output_push.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  @Test
  public void testRexExplain() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | rex field=lastname \\\"(?<initial>^[A-Z])\\\" |"
            + " head 5";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_rex.yaml");
    assertYamlEqualsJsonIgnoreId(expected, result);
  }

  @Test
  public void testExplainAppendCommand() throws IOException {
    String expected = loadExpectedPlan("explain_append_command.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                Locale.ROOT,
                "source=%s | stats count(balance) as cnt by gender | append [ source=%s | stats"
                    + " count() as cnt ]",
                TEST_INDEX_BANK,
                TEST_INDEX_BANK)));
  }

  @Test
  public void testMvjoinExplain() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | eval result = mvjoin(array('a', 'b', 'c'), ',')"
            + " | fields result | head 1";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_mvjoin.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  @Test
  public void testPreventLimitPushdown() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    setMaxResultWindow("opensearch-sql_test_index_account", 1);
    String query = "source=opensearch-sql_test_index_account | head 1 from 1";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_prevent_limit_push.yaml");
    assertYamlEqualsJsonIgnoreId(expected, result);
    resetMaxResultWindow("opensearch-sql_test_index_account");
  }

  @Test
  public void testPushdownLimitIntoAggregation() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_limit_agg_pushdown.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString("source=opensearch-sql_test_index_account | stats count() by state"));

    expected = loadExpectedPlan("explain_limit_agg_pushdown2.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats count() by state | head 100"));

    expected = loadExpectedPlan("explain_limit_agg_pushdown3.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats count() by state | head 100 | head 10"
                + " from 10 "));

    expected = loadExpectedPlan("explain_limit_agg_pushdown4.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats count() by state | sort state | head"
                + " 100 | head 10 from 10 "));

    expected = loadExpectedPlan("explain_limit_agg_pushdown_bucket_nullable1.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats bucket_nullable=false count() by"
                + " state | head 100 | head 10 from 10 "));

    expected = loadExpectedPlan("explain_limit_agg_pushdown_bucket_nullable2.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats bucket_nullable=false count() by"
                + " state | sort state | head 100 | head 10 from 10 "));

    // Don't pushdown the combination of limit and sort
    expected = loadExpectedPlan("explain_limit_agg_pushdown5.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats count() by state | sort `count()` |"
                + " head 100 | head 10 from 10 "));
  }

  @Test
  public void testExplainMaxOnStringField() throws IOException {
    String expected = loadExpectedPlan("explain_max_string_field.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString("source=opensearch-sql_test_index_account | stats max(firstname)"));
  }

  @Test
  public void testExplainMinOnStringField() throws IOException {
    String expected = loadExpectedPlan("explain_min_string_field.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString("source=opensearch-sql_test_index_account | stats min(firstname)"));
  }

  @Test
  @Override
  public void testCountAggPushDownExplain() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    // should be optimized by hits.total.value
    String expected = loadExpectedPlan("explain_count_agg_push1.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString("source=opensearch-sql_test_index_account | stats count() as cnt"));

    // should be optimized
    expected = loadExpectedPlan("explain_count_agg_push2.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats count(lastname) as cnt"));

    // should be optimized
    expected = loadExpectedPlan("explain_count_agg_push3.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | eval name = lastname | stats count(name) as"
                + " cnt"));

    // should be optimized
    expected = loadExpectedPlan("explain_count_agg_push4.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats count() as c1, count() as c2"));

    // should be optimized
    expected = loadExpectedPlan("explain_count_agg_push5.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats count(lastname) as c1,"
                + " count(lastname) as c2"));

    // should be optimized
    expected = loadExpectedPlan("explain_count_agg_push6.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | eval name = lastname | stats"
                + " count(lastname), count(name)"));

    // should not be optimized
    expected = loadExpectedPlan("explain_count_agg_push7.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats count(balance + 1) as cnt"));

    // should not be optimized
    expected = loadExpectedPlan("explain_count_agg_push8.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats count() as c1, count(lastname) as"
                + " c2"));

    // should not be optimized
    expected = loadExpectedPlan("explain_count_agg_push9.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats count(firstname), count(lastname)"));

    // should not be optimized
    expected = loadExpectedPlan("explain_count_agg_push10.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | eval name = lastname | stats"
                + " count(firstname), count(name)"));
  }

  @Test
  public void testExplainCountsByAgg() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_agg_counts_by1.yaml");
    // case of only count(): doc_count works
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | stats count(), count() as c1 by gender", TEST_INDEX_ACCOUNT)));

    // count(FIELD) by: doc_count doesn't work
    expected = loadExpectedPlan("explain_agg_counts_by2.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | stats count(balance) as c1, count(balance) as c2 by gender",
                TEST_INDEX_ACCOUNT)));

    // count(FIELD) by: doc_count doesn't work
    expected = loadExpectedPlan("explain_agg_counts_by3.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | eval account_number_alias = account_number"
                    + " | stats count(account_number), count(account_number_alias) as c2 by gender",
                TEST_INDEX_ACCOUNT)));

    // count() + count(FIELD)): doc_count doesn't work
    expected = loadExpectedPlan("explain_agg_counts_by4.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | stats count(), count(account_number) by gender", TEST_INDEX_ACCOUNT)));

    // count(FIELD1) + count(FIELD2)) by: doc_count doesn't work
    expected = loadExpectedPlan("explain_agg_counts_by5.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | stats count(balance), count(account_number) by gender",
                TEST_INDEX_ACCOUNT)));

    // case of count(EXPRESSION) by: doc_count doesn't work
    expected = loadExpectedPlan("explain_agg_counts_by6.yaml");
    assertYamlEqualsJsonIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | eval b_1 = balance + 1"
                    + " | stats count(b_1), count(pow(balance, 2)) as c3 by gender",
                TEST_INDEX_ACCOUNT)));
  }

  @Test
  public void testExplainSortOnMetricsNoBucketNullable() throws IOException {
    // TODO enhancement later: https://github.com/opensearch-project/sql/issues/4282
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_agg_sort_on_metrics1.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats bucket_nullable=false count() by"
                + " state | sort `count()`"));

    expected = loadExpectedPlan("explain_agg_sort_on_metrics2.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats bucket_nullable=false count() by"
                + " gender, state | sort `count()`"));
  }

  /**
   * Executes the PPL query and returns the result as a string with windows-style line breaks
   * replaced with Unix-style ones.
   *
   * @param ppl the PPL query to execute
   * @return the result of the query as a string with line breaks replaced
   * @throws IOException if an error occurs during query execution
   */
  private String executeWithReplace(String ppl) throws IOException {
    var result = executeQueryToString(ppl);
    return result.replace("\\r\\n", "\\n");
  }

  @Test
  public void testStrftimeFunctionExplain() throws IOException {
    // Test explain for strftime function
    String query =
        "source=opensearch-sql_test_index_account | eval formatted_date = strftime(1521467703,"
            + " '%Y-%m-%d') | fields formatted_date | head 1";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_strftime_function.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  // Script generation is not stable in v2
  @Test
  public void testExplainPushDownScriptsContainingUDT() throws IOException {
    assertJsonEqualsIgnoreId(
        loadExpectedPlan("explain_filter_script_ip_push.json"),
        explainQueryToString(
            String.format(
                "source=%s | where cidrmatch(host, '0.0.0.0/24') | fields host",
                TEST_INDEX_WEBLOGS)));

    assertJsonEqualsIgnoreId(
        loadExpectedPlan("explain_agg_script_timestamp_push.json"),
        explainQueryToString(
            String.format(
                "source=%s | eval t = unix_timestamp(birthdate) | stats count() by t | sort t |"
                    + " head 3",
                TEST_INDEX_BANK)));

    assertJsonEqualsIgnoreId(
        loadExpectedPlan("explain_agg_script_udt_arg_push.json"),
        explainQueryToString(
            String.format(
                "source=%s | eval t = date_add(birthdate, interval 1 day) | stats count() by"
                    + " span(t, 1d)",
                TEST_INDEX_BANK)));
  }
}
