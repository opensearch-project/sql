/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestUtils.*;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_LOGS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_SIMPLE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STRINGS;
import static org.opensearch.sql.util.MatcherUtils.assertJsonEqualsIgnoreId;

import java.io.IOException;
import java.util.Locale;
import org.junit.Assume;
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
    String expected = loadExpectedPlan("explain_sarg_filter_push_single_range.json");
    assertJsonEqualsIgnoreId(expected, result);
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
    Assume.assumeTrue("This test is only for push down enabled", isPushdownEnabled());
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
    Assume.assumeTrue("This test is only for push down enabled", isPushdownEnabled());
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
    Assume.assumeTrue("This test is only for push down enabled", isPushdownEnabled());
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
    Assume.assumeTrue("This test is only for push down enabled", isPushdownEnabled());
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
        isPushdownEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_timechart.json")
            : loadFromFile("expectedOutput/calcite/explain_timechart_no_pushdown.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  @Test
  public void testExplainWithTimechartCount() throws IOException {
    var result = explainQueryToString("source=events | timechart span=1m count() by host");
    String expected =
        isPushdownEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_timechart_count.json")
            : loadFromFile("expectedOutput/calcite/explain_timechart_count_no_pushdown.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  @Test
  public void noPushDownForAggOnWindow() throws IOException {
    Assume.assumeTrue("This test is only for push down enabled", isPushdownEnabled());
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
    Assume.assumeTrue("This test is only for push down enabled", isPushdownEnabled());
    String result =
        explainQueryToString(
            "explain source=opensearch-sql_test_index_account | where length(address) > 0 | eval"
                + " address_length = length(address) | stats count() by address_length");
    String expected = loadFromFile("expectedOutput/calcite/explain_script_push_on_text.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  @Test
  public void testEventstatsDistinctCountExplain() throws IOException {
    Assume.assumeTrue("This test is only for push down enabled", isPushdownEnabled());
    String query =
        "source=opensearch-sql_test_index_account | eventstats dc(state) as distinct_states";
    var result = explainQueryToString(query);
    String expected = loadFromFile("expectedOutput/calcite/explain_eventstats_dc.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  @Test
  public void testEventstatsDistinctCountFunctionExplain() throws IOException {
    Assume.assumeTrue("This test is only for push down enabled", isPushdownEnabled());
    String query =
            "source=opensearch-sql_test_index_account | eventstats distinct_count(state) as"
                    + " distinct_states by gender";
    var result = explainQueryToString(query);
    String expected = loadFromFile("expectedOutput/calcite/explain_eventstats_distinct_count.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  public void testExplainBinWithBins() throws IOException {
    String expected = loadExpectedPlan("explain_bin_bins.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString("source=opensearch-sql_test_index_account | bin age bins=3 | head 5"));
  }

  @Test
  public void testExplainBinWithSpan() throws IOException {
    String expected = loadExpectedPlan("explain_bin_span.json");
    assertJsonEqualsIgnoreId(
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
    String expected = loadExpectedPlan("explain_bin_aligntime.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_time_data | bin @timestamp span=2h aligntime=latest |"
                + " head 5"));
  }

  // Only for Calcite, as v2 gets unstable serialized string for function
  @Test
  public void testExplainOnAggregationWithSumEnhancement() throws IOException {
    String expected = loadExpectedPlan("explain_agg_with_sum_enhancement.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | stats sum(balance), sum(balance + 100), sum(balance - 100),"
                    + " sum(balance * 100), sum(balance / 100) by gender",
                TEST_INDEX_BANK)));
  }

  @Test
  public void testExplainRegexMatchInWhereWithScriptPushdown() throws IOException {
    Assume.assumeTrue("This test is only for push down enabled", isPushdownEnabled());
    String query =
        String.format("source=%s | where regex_match(name, 'hello')", TEST_INDEX_STRINGS);
    var result = explainQueryToString(query);
    String expected = loadFromFile("expectedOutput/calcite/explain_regex_match_in_where.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  @Test
  public void testExplainRegexMatchInEvalWithOutScriptPushdown() throws IOException {
    Assume.assumeTrue("This test is only for push down enabled", isPushdownEnabled());
    String query =
        String.format(
            "source=%s |eval has_hello = regex_match(name, 'hello') | fields has_hello",
            TEST_INDEX_STRINGS);
    var result = explainQueryToString(query);
    String expected = loadFromFile("expectedOutput/calcite/explain_regex_match_in_eval.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  @Test
  public void testRegexExplain() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | regex lastname='^[A-Z][a-z]+$' | head 5";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_regex.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  @Test
  public void testRegexNegatedExplain() throws IOException {
    String query = "source=opensearch-sql_test_index_account | regex lastname!='.*son$' | head 5";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_regex_negated.json");
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

  @Test
  public void testListAggregationExplain() throws IOException {
    String expected = loadExpectedPlan("explain_list_aggregation.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats list(age) as age_list"));
  }

  @Test
  public void testRexExplain() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | rex field=lastname \\\"(?<initial>^[A-Z])\\\" |"
            + " head 5";
    var result = explainQueryToString(query);
    String expected = loadExpectedPlan("explain_rex.json");
    assertJsonEqualsIgnoreId(expected, result);
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
}
