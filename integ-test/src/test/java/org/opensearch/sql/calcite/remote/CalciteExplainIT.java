/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ALIAS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CASCADED_NESTED;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DEEP_NESTED;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_LOGS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_SIMPLE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_OTEL_LOGS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STRINGS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_TIME_DATA;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WEBLOGS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WORKER;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WORK_INFORMATION;
import static org.opensearch.sql.util.MatcherUtils.assertJsonEqualsIgnoreId;
import static org.opensearch.sql.util.MatcherUtils.assertYamlEqualsIgnoreId;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;

import java.io.IOException;
import java.util.Locale;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.ppl.ExplainIT;
import org.opensearch.sql.protocol.response.format.Format;

public class CalciteExplainIT extends ExplainIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    setQueryBucketSize(1000);
    loadIndex(Index.STRINGS);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.NESTED_SIMPLE);
    loadIndex(Index.TIME_TEST_DATA);
    loadIndex(Index.TIME_TEST_DATA2);
    loadIndex(Index.EVENTS);
    loadIndex(Index.LOGS);
    loadIndex(Index.WORKER);
    loadIndex(Index.WORK_INFORMATION);
    loadIndex(Index.WEBLOG);
    loadIndex(Index.DATA_TYPE_ALIAS);
    loadIndex(Index.DEEP_NESTED);
    loadIndex(Index.CASCADED_NESTED);
  }

  @Override
  @Ignore("test only in v2")
  public void testExplainModeUnsupportedInV2() throws IOException {}

  // Only for Calcite
  @Test
  public void supportSearchSargPushDown_singleRange() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | where age >= 1.0 and age < 10 | fields age";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_sarg_filter_push_single_range.yaml");
    assertYamlEqualsIgnoreId(expected, result);
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
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_sarg_filter_push_time_range.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  // Only for Calcite
  @Test
  public void testJoinWithCriteriaAndMaxOption() throws IOException {
    String query =
        "source=opensearch-sql_test_index_bank | join max=1 left=l right=r on"
            + " l.account_number=r.account_number opensearch-sql_test_index_bank";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_join_with_criteria_max_option.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  // Only for Calcite
  @Test
  public void testJoinWithFieldListAndMaxOption() throws IOException {
    String query =
        "source=opensearch-sql_test_index_bank | join type=inner max=1 account_number"
            + " opensearch-sql_test_index_bank";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_join_with_fields_max_option.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  // Only for Calcite
  @Test
  public void testJoinWithFieldList() throws IOException {
    String query =
        "source=opensearch-sql_test_index_bank | join type=outer account_number"
            + " opensearch-sql_test_index_bank";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_join_with_fields.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testExplainExistsUncorrelatedSubquery() throws IOException {
    String expected = loadExpectedPlan("explain_exists_uncorrelated_subquery.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source = %s"
                    + "| where exists ["
                    + "    source = %s | where name = 'Tom'"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION)));
  }

  @Test
  public void testExplainExistsCorrelatedSubquery() throws IOException {
    String expected = loadExpectedPlan("explain_exists_correlated_subquery.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source = %s"
                    + "| where exists ["
                    + "    source = %s | where id = uid and name = 'Tom'"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION)));
  }

  @Test
  public void testExplainInUncorrelatedSubquery() throws IOException {
    String expected = loadExpectedPlan("explain_in_uncorrelated_subquery.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source = %s"
                    + "| where id in ["
                    + "    source = %s | fields uid"
                    + "  ]"
                    + "| sort  - salary"
                    + "| fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION)));
  }

  @Test
  public void testExplainInCorrelatedSubquery() throws IOException {
    String expected = loadExpectedPlan("explain_in_correlated_subquery.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source = %s"
                    + "| where name in ["
                    + "    source = %s | where id = uid and name = 'Tom' | fields name"
                    + "  ]"
                    + "| sort - salary | fields id, name, salary",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION)));
  }

  @Test
  public void testExplainScalarUncorrelatedSubqueryInSelect() throws IOException {
    String expected = loadExpectedPlan("explain_scalar_uncorrelated_subquery_in_select.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source = %s"
                    + "| eval count_dept = ["
                    + "    source = %s | stats count(name)"
                    + "  ]"
                    + "| fields name, count_dept",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION, TEST_INDEX_WORK_INFORMATION)));
  }

  @Test
  public void testExplainScalarUncorrelatedSubqueryInWhere() throws IOException {
    String expected = loadExpectedPlan("explain_scalar_uncorrelated_subquery_in_where.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source = %s"
                    + "| where id > ["
                    + "    source = %s | stats count(name)"
                    + "  ] + 999"
                    + "| fields name",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION, TEST_INDEX_WORK_INFORMATION)));
  }

  @Test
  public void testExplainScalarCorrelatedSubqueryInSelect() throws IOException {
    String expected = loadExpectedPlan("explain_scalar_correlated_subquery_in_select.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source = %s"
                    + "| eval count_dept = ["
                    + "    source = %s"
                    + "    | where id = uid | stats count(name)"
                    + "  ]"
                    + "| fields id, name, count_dept",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION)));
  }

  @Test
  public void testExplainScalarCorrelatedSubqueryInWhere() throws IOException {
    String expected = loadExpectedPlan("explain_scalar_correlated_subquery_in_where.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source = %s"
                    + "| where id = ["
                    + "    source = %s | where id = uid | stats max(uid)"
                    + "  ]"
                    + "| fields id, name",
                TEST_INDEX_WORKER, TEST_INDEX_WORK_INFORMATION)));
  }

  // Only for Calcite
  @Test
  public void supportPushDownSortMergeJoin() throws IOException {
    String query =
        "source=opensearch-sql_test_index_bank| join left=l right=r on"
            + " l.account_number=r.account_number opensearch-sql_test_index_bank";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_merge_join_sort_push.yaml");
    assertYamlEqualsIgnoreId(expected, result);
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
    String expected = loadExpectedPlan("explain_isempty.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml("source=opensearch-sql_test_index_account | where isempty(firstname)"));
  }

  @Test
  public void testExplainMultisearchBasic() throws IOException {
    String query =
        "| multisearch [search"
            + " source=opensearch-sql_test_index_account | where age < 30 | eval age_group ="
            + " 'young'] [search source=opensearch-sql_test_index_account | where age >= 30 | eval"
            + " age_group = 'adult'] | stats count by age_group";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_multisearch_basic.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testExplainMultisearchTimestampInterleaving() throws IOException {
    String query =
        "| multisearch "
            + "[search source=opensearch-sql_test_index_time_data | where category IN ('A', 'B')] "
            + "[search source=opensearch-sql_test_index_time_data2 | where category IN ('E', 'F')] "
            + "| head 5";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_multisearch_timestamp.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  // Only for Calcite
  @Test
  public void testExplainIsBlank() throws IOException {
    // script pushdown
    String expected = loadExpectedPlan("explain_isblank.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml("source=opensearch-sql_test_index_account | where isblank(firstname)"));
  }

  // Only for Calcite
  @Test
  public void testExplainIsEmptyOrOthers() throws IOException {
    // script pushdown
    String expected = loadExpectedPlan("explain_isempty_or_others.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
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
    var result = explainQueryYaml(query, ExplainMode.EXTENDED);
    String expected = loadFromFile("expectedOutput/calcite/explain_skip_script_encoding.yaml");
    assertYamlEqualsIgnoreId(expected, result);
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
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
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
    var result = explainQueryYaml("source=events | timechart span=1m avg(cpu_usage) by host");
    String expected = loadExpectedPlan("explain_timechart.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testExplainWithTimechartCount() throws IOException {
    var result = explainQueryYaml("source=events | timechart span=1m count() by host");
    String expected = loadExpectedPlan("explain_timechart_count.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testExplainTimechartPerSecond() throws IOException {
    var result = explainQueryToString("source=events | timechart span=2m per_second(cpu_usage)");
    assertTrue(
        result.contains(
            "per_second(cpu_usage)=[DIVIDE(*($1, 1000.0E0), TIMESTAMPDIFF('MILLISECOND':VARCHAR,"
                + " $0, TIMESTAMPADD('MINUTE':VARCHAR, 2, $0)))]"));
    assertTrue(result.contains("per_second(cpu_usage)=[SUM($0)]"));
  }

  @Test
  public void testExplainTimechartPerMinute() throws IOException {
    var result = explainQueryToString("source=events | timechart span=2m per_minute(cpu_usage)");
    assertTrue(
        result.contains(
            "per_minute(cpu_usage)=[DIVIDE(*($1, 60000.0E0), TIMESTAMPDIFF('MILLISECOND':VARCHAR,"
                + " $0, TIMESTAMPADD('MINUTE':VARCHAR, 2, $0)))]"));
    assertTrue(result.contains("per_minute(cpu_usage)=[SUM($0)]"));
  }

  @Test
  public void testExplainTimechartPerHour() throws IOException {
    var result = explainQueryToString("source=events | timechart span=2m per_hour(cpu_usage)");
    assertTrue(
        result.contains(
            "per_hour(cpu_usage)=[DIVIDE(*($1, 3600000.0E0), TIMESTAMPDIFF('MILLISECOND':VARCHAR,"
                + " $0, TIMESTAMPADD('MINUTE':VARCHAR, 2, $0)))]"));
    assertTrue(result.contains("per_hour(cpu_usage)=[SUM($0)]"));
  }

  @Test
  public void testExplainTimechartPerDay() throws IOException {
    var result = explainQueryToString("source=events | timechart span=2m per_day(cpu_usage)");
    assertTrue(
        result.contains(
            "per_day(cpu_usage)=[DIVIDE(*($1, 8.64E7), TIMESTAMPDIFF('MILLISECOND':VARCHAR, $0,"
                + " TIMESTAMPADD('MINUTE':VARCHAR, 2, $0)))]"));
    assertTrue(result.contains("per_day(cpu_usage)=[SUM($0)]"));
  }

  @Test
  public void noPushDownForAggOnWindow() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String query =
        "source=opensearch-sql_test_index_account | patterns address method=BRAIN  | stats count()"
            + " by patterns_field";
    var result = explainQueryYaml(query);
    String expected = loadFromFile("expectedOutput/calcite/explain_agg_on_window.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  // Only for Calcite
  @Test
  public void supportPushDownScriptOnTextField() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String result =
        explainQueryYaml(
            "explain source=opensearch-sql_test_index_account | where length(address) > 0 | eval"
                + " address_length = length(address) | stats count() by address_length");
    String expected = loadFromFile("expectedOutput/calcite/explain_script_push_on_text.yaml");
    assertYamlEqualsIgnoreId(expected, result);
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
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml("source=events | bin @timestamp bins=3 | stats count() by @timestamp"));

    expected = loadExpectedPlan("explain_stats_bins_on_time2.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=events | bin @timestamp bins=3 | stats avg(cpu_usage) by @timestamp"));
  }

  @Test
  public void testExplainStatsWithSubAggregation() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_stats_bins_on_time_and_term.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=events | bin @timestamp bins=3 | stats bucket_nullable=false count() by"
                + " @timestamp, region"));

    expected = loadExpectedPlan("explain_stats_bins_on_time_and_term2.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=events | bin @timestamp bins=3 | stats bucket_nullable=false avg(cpu_usage) by"
                + " @timestamp, region"));
  }

  @Test
  public void testExplainBinWithSpan() throws IOException {
    String expected = loadExpectedPlan("explain_bin_span.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml("source=opensearch-sql_test_index_account | bin age span=10 | head 5"));
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
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
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

  @Test
  public void testEventstatsNullBucketExplain() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | eventstats bucket_nullable=false count() by"
            + " state";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_eventstats_null_bucket.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testStreamstatsDistinctCountExplain() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | streamstats dc(state) as distinct_states";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_streamstats_dc.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testStreamstatsDistinctCountFunctionExplain() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | streamstats distinct_count(state) as"
            + " distinct_states by gender";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_streamstats_distinct_count.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testStreamstatsGlobalExplain() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | streamstats window=2 global=true avg(age) as"
            + " avg_age by gender";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_streamstats_global.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testStreamstatsResetExplain() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | streamstats current=false reset_before=age>34"
            + " reset_after=age<25 avg(age) as avg_age by gender";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_streamstats_reset.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testStreamstatsNullBucketExplain() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | streamstats bucket_nullable=false avg(age) as"
            + " avg_age by gender";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_streamstats_null_bucket.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testStreamstatsGlobalNullBucketExplain() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | streamstats bucket_nullable=false window=2"
            + " global=true avg(age) as avg_age by gender";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_streamstats_global_null_bucket.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testStreamstatsResetNullBucketExplain() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | streamstats bucket_nullable=false current=false"
            + " reset_before=age>34 reset_after=age<25 avg(age) as avg_age by gender";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_streamstats_reset_null_bucket.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testKeywordILikeFunctionExplain() throws IOException {
    // ilike is only supported in v3
    String expected = loadExpectedPlan("explain_keyword_ilike_function.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | where ilike(firstname, '%mbe%')"));
  }

  @Test
  public void testTextILikeFunctionExplain() throws IOException {
    // ilike is only supported in v3
    String expected = loadExpectedPlan("explain_text_ilike_function.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | where ilike(address, '%Holmes%')"));
  }

  // Only for Calcite, as v2 gets unstable serialized string for function
  @Test
  public void testExplainOnAggregationWithSumEnhancement() throws IOException {
    String expected = loadExpectedPlan("explain_agg_with_sum_enhancement.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | stats sum(balance), sum(balance + 100), sum(balance - 100),"
                    + " sum(balance * 100), sum(balance / 100) by gender",
                TEST_INDEX_BANK)));
  }

  @Test
  public void testStatsDistinctCountApproxFunctionExplainWithPushDown() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String query =
        "source=opensearch-sql_test_index_account | stats distinct_count_approx(state) as"
            + " distinct_states by gender";
    var result = explainQueryToString(query);
    String expected =
        loadFromFile(
            "expectedOutput/calcite/explain_agg_with_distinct_count_approx_enhancement.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  @Test
  public void testExplainRegexMatchInWhereWithScriptPushdown() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String query =
        String.format("source=%s | where regexp_match(name, 'hello')", TEST_INDEX_STRINGS);
    var result = explainQueryYaml(query);
    String expected = loadFromFile("expectedOutput/calcite/explain_regexp_match_in_where.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testExplainRegexMatchInEvalWithOutScriptPushdown() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String query =
        String.format(
            "source=%s |eval has_hello = regexp_match(name, 'hello') | fields has_hello",
            TEST_INDEX_STRINGS);
    var result = explainQueryToString(query);
    String expected = loadFromFile("expectedOutput/calcite/explain_regexp_match_in_eval.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  // Only for Calcite
  @Test
  public void testExplainOnEarliestLatest() throws IOException {
    String expected = loadExpectedPlan("explain_earliest_latest.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | stats earliest(message) as earliest_message, latest(message) as"
                    + " latest_message by server",
                TEST_INDEX_LOGS)));
  }

  // Only for Calcite
  @Test
  public void testExplainOnEarliestLatestWithCustomTimeField() throws IOException {
    String expected = loadExpectedPlan("explain_earliest_latest_custom_time.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | stats earliest(message, created_at) as earliest_message,"
                    + " latest(message, created_at) as latest_message by level",
                TEST_INDEX_LOGS)));
  }

  // Only for Calcite
  @Test
  public void testExplainOnFirstLast() throws IOException {
    String expected = loadExpectedPlan("explain_first_last.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | stats first(firstname) as first_name, last(firstname) as"
                    + " last_name by gender",
                TEST_INDEX_BANK)));
  }

  // Only for Calcite
  @Test
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
  public void testExplainOnStreamstatsEarliestLatest() throws IOException {
    String expected = loadExpectedPlan("explain_streamstats_earliest_latest.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | streamstats earliest(message) as earliest_message, latest(message) as"
                    + " latest_message by server",
                TEST_INDEX_LOGS)));
  }

  @Test
  public void testExplainOnStreamstatsEarliestLatestWithCustomTimeField() throws IOException {
    String expected = loadExpectedPlan("explain_streamstats_earliest_latest_custom_time.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | streamstats earliest(message, created_at) as earliest_message,"
                    + " latest(message, created_at) as latest_message by level",
                TEST_INDEX_LOGS)));
  }

  @Test
  public void testExplainOnStreamstatsEarliestLatestNoGroupBy() throws IOException {
    String expected = loadExpectedPlan("explain_streamstats_earliest_latest_no_group.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | streamstats earliest(message) as earliest_message, latest(message) as"
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
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_regex.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testRegexNegatedExplain() throws IOException {
    String query = "source=opensearch-sql_test_index_account | regex lastname!='.*son$' | head 5";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_regex_negated.yaml");
    assertYamlEqualsIgnoreId(expected, result);
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
  public void testComplexSortExpressionPushDownExplain() throws Exception {
    String query =
        "source=opensearch-sql_test_index_bank| eval age2 = age + balance | sort age2 | fields age,"
            + " age2";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_complex_sort_expr_push.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testComplexSortExpressionPushDownWithOnlyExprProjected() throws Exception {
    String query =
        "source=opensearch-sql_test_index_bank| eval age2 = age + balance | sort age2 | fields"
            + " age2";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_complex_sort_expr_single_expr_output_push.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testComplexSortExpressionPushDownWithoutExprProjected() throws Exception {
    String query =
        "source=opensearch-sql_test_index_bank| eval age2 = age + balance | sort age2 | fields age";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_complex_sort_expr_no_expr_output_push.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testComplexSortExpressionProjectThenSort() throws Exception {
    String query =
        "source=opensearch-sql_test_index_bank| eval age2 = age + balance | fields age, age2 | sort"
            + " age2";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_complex_sort_expr_project_then_sort.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  /*
   * TODO: A potential optimization is to leverage RexSimplify to simplify -(+($10, $7), $10) to $7
   * Above simplification can only work when $10 is nonnull and there is no precision loss of
   * expression calculation
   */
  @Test
  public void testSortNestedComplexExpression() throws Exception {
    String query =
        "source=opensearch-sql_test_index_bank| eval age2 = age + balance, age3 = age2 - age | sort"
            + " age3";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_complex_sort_nested_expr.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testSortComplexExpressionThenSortField() throws Exception {
    String query =
        "source=opensearch-sql_test_index_bank| eval age2 = age + balance | sort age2, age | eval"
            + " balance2 = abs(balance) | sort age";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_complex_sort_then_field_sort.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testSortComplexExprMixedWithSimpleExpr() throws Exception {
    String query =
        "source=opensearch-sql_test_index_bank| eval age2 = age + balance, balance2 = balance + 1 |"
            + " sort age2, balance2 ";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_sort_complex_and_simple_expr.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testComplexSortExprPushdownForSMJ() throws Exception {
    String query =
        "source=opensearch-sql_test_index_bank | rex field=lastname \\\"(?<initial>^[A-Z])\\\" |"
            + " join left=a right=b on a.initial = b.firstname opensearch-sql_test_index_bank";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_complex_sort_expr_pushdown_for_smj.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testSimpleSortExprPushdownForSMJ() throws Exception {
    String query =
        "source=opensearch-sql_test_index_bank | join left=a right=b on a.age + 1 = b.balance - 20"
            + " opensearch-sql_test_index_bank";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_simple_sort_expr_pushdown_for_smj.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testSortPassThroughJoinThenPushdown() throws Exception {
    String query =
        "source=opensearch-sql_test_index_bank | rex field=lastname \\\"(?<initial>^[A-Z])\\\" |"
            + " join type=left left=a right=b on a.initial = b.firstname"
            + " opensearch-sql_test_index_bank | sort initial";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_sort_pass_through_join_then_pushdown.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testComplexSortExprPushdownForSMJWithMaxOption() throws Exception {
    String query =
        "source=opensearch-sql_test_index_bank | rex field=lastname \\\"(?<lastname>^[A-Z])\\\" |"
            + " join type=left max=1 lastname opensearch-sql_test_index_bank";
    var result = explainQueryYaml(query);
    String expected =
        loadExpectedPlan("explain_complex_sort_expr_pushdown_for_smj_w_max_option.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testRexExplain() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | rex field=lastname \\\"(?<initial>^[A-Z])\\\" |"
            + " head 5";
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_rex.yaml");
    assertYamlEqualsIgnoreId(expected, result);
  }

  @Test
  public void testExplainAppendCommand() throws IOException {
    String expected = loadExpectedPlan("explain_append_command.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                Locale.ROOT,
                "source=%s | stats count(balance) as cnt by gender | append [ source=%s | stats"
                    + " count() as cnt ]",
                TEST_INDEX_BANK,
                TEST_INDEX_BANK)));
  }

  @Test
  public void testExplainAppendPipeCommand() throws IOException {
    String expected = loadExpectedPlan("explain_appendpipe_command.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                Locale.ROOT,
                "source=%s | appendpipe [ stats count(balance) as cnt by gender  ]",
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
    var result = explainQueryYaml(query);
    String expected = loadExpectedPlan("explain_prevent_limit_push.yaml");
    assertYamlEqualsIgnoreId(expected, result);
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
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | stats count() by state | head 100"));

    expected = loadExpectedPlan("explain_limit_agg_pushdown3.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account | stats count() by state | head 100 | head 10"
                + " from 10 "));

    expected = loadExpectedPlan("explain_limit_agg_pushdown4.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | stats count() by state | sort state | head"
                + " 100 | head 10 from 10 "));

    expected = loadExpectedPlan("explain_limit_agg_pushdown_bucket_nullable1.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | stats bucket_nullable=false count() by"
                + " state | head 100 | head 10 from 10 "));

    expected = loadExpectedPlan("explain_limit_agg_pushdown_bucket_nullable2.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
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
    String expected = loadExpectedPlan("explain_max_string_field.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml("source=opensearch-sql_test_index_account | stats max(firstname)"));
  }

  @Test
  public void testExplainMinOnStringField() throws IOException {
    String expected = loadExpectedPlan("explain_min_string_field.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml("source=opensearch-sql_test_index_account | stats min(firstname)"));
  }

  @Test
  @Override
  public void testCountAggPushDownExplain() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    // should be optimized by hits.total.value
    String expected = loadExpectedPlan("explain_count_agg_push1.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml("source=opensearch-sql_test_index_account | stats count() as cnt"));

    // should be optimized
    expected = loadExpectedPlan("explain_count_agg_push2.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | stats count(lastname) as cnt"));

    // should be optimized
    expected = loadExpectedPlan("explain_count_agg_push3.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | eval name = lastname | stats count(name) as"
                + " cnt"));

    // should be optimized
    expected = loadExpectedPlan("explain_count_agg_push4.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | stats count() as c1, count() as c2"));

    // should be optimized
    expected = loadExpectedPlan("explain_count_agg_push5.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | stats count(lastname) as c1,"
                + " count(lastname) as c2"));

    // should be optimized
    expected = loadExpectedPlan("explain_count_agg_push6.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | eval name = lastname | stats"
                + " count(lastname), count(name)"));

    // should not be optimized
    expected = loadExpectedPlan("explain_count_agg_push7.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | stats count(balance + 1) as cnt"));

    // should not be optimized
    expected = loadExpectedPlan("explain_count_agg_push8.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | stats count() as c1, count(lastname) as"
                + " c2"));

    // should not be optimized
    expected = loadExpectedPlan("explain_count_agg_push9.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | stats count(firstname), count(lastname)"));

    // should not be optimized
    expected = loadExpectedPlan("explain_count_agg_push10.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | eval name = lastname | stats"
                + " count(firstname), count(name)"));
  }

  @Test
  public void testExplainCountsByAgg() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_agg_counts_by1.yaml");
    // case of only count(): doc_count works
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | stats count(), count() as c1 by gender", TEST_INDEX_ACCOUNT)));

    // count(FIELD) by: doc_count doesn't work
    expected = loadExpectedPlan("explain_agg_counts_by2.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | stats count(balance) as c1, count(balance) as c2 by gender",
                TEST_INDEX_ACCOUNT)));

    // count(FIELD) by: doc_count doesn't work
    expected = loadExpectedPlan("explain_agg_counts_by3.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | eval account_number_alias = account_number"
                    + " | stats count(account_number), count(account_number_alias) as c2 by gender",
                TEST_INDEX_ACCOUNT)));

    // count() + count(FIELD)): doc_count doesn't work
    expected = loadExpectedPlan("explain_agg_counts_by4.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | stats count(), count(account_number) by gender", TEST_INDEX_ACCOUNT)));

    // count(FIELD1) + count(FIELD2)) by: doc_count doesn't work
    expected = loadExpectedPlan("explain_agg_counts_by5.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | stats count(balance), count(account_number) by gender",
                TEST_INDEX_ACCOUNT)));

    // case of count(EXPRESSION) by: doc_count doesn't work
    expected = loadExpectedPlan("explain_agg_counts_by6.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | eval b_1 = balance + 1"
                    + " | stats count(b_1), count(pow(balance, 2)) as c3 by gender",
                TEST_INDEX_ACCOUNT)));
  }

  @Test
  public void testPaginatingAggForHaving() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    try {
      setQueryBucketSize(2);
      String expected = loadExpectedPlan("explain_agg_paginating_having1.yaml");
      assertYamlEqualsIgnoreId(
          expected,
          explainQueryYaml(
              "source=opensearch-sql_test_index_account | stats count() as c by"
                  + " state | where c > 10"));
      expected = loadExpectedPlan("explain_agg_paginating_having2.yaml");
      assertYamlEqualsIgnoreId(
          expected,
          explainQueryYaml(
              "source=opensearch-sql_test_index_account | stats bucket_nullable = false count() by"
                  + " state | where `count()` > 10"));
      expected = loadExpectedPlan("explain_agg_paginating_having3.yaml");
      assertYamlEqualsIgnoreId(
          expected,
          explainQueryYaml(
              "source=opensearch-sql_test_index_account | stats avg(balance) as avg, count() as cnt"
                  + " by state | eval new_avg = avg + 1000, new_cnt = cnt + 1 | where new_avg >"
                  + " 1000 or new_cnt > 1"));
    } finally {
      resetQueryBucketSize();
    }
  }

  @Test
  public void testPaginatingAggForJoin() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    try {
      setQueryBucketSize(2);
      String expected = loadExpectedPlan("explain_agg_paginating_join1.yaml");
      assertYamlEqualsIgnoreId(
          expected,
          explainQueryYaml(
              "source=opensearch-sql_test_index_account | stats count() as c by state | join left=l"
                  + " right=r on l.state=r.state [ source=opensearch-sql_test_index_bank | stats"
                  + " count() as c by state ]"));
      expected = loadExpectedPlan("explain_agg_paginating_join2.yaml");
      assertYamlEqualsIgnoreId(
          expected,
          explainQueryYaml(
              "source=opensearch-sql_test_index_account | stats bucket_nullable = false count() as"
                  + " c by state | join left=l right=r on l.state=r.state ["
                  + " source=opensearch-sql_test_index_bank | stats bucket_nullable = false"
                  + " count() as c by state ]"));
      expected = loadExpectedPlan("explain_agg_paginating_join3.yaml");
      assertYamlEqualsIgnoreId(
          expected,
          explainQueryYaml(
              "source=opensearch-sql_test_index_account | stats count() as c by state | join"
                  + " type=inner state [ source=opensearch-sql_test_index_bank | stats count()"
                  + " as c by state ]"));
      expected = loadExpectedPlan("explain_agg_paginating_join4.yaml");
      assertYamlEqualsIgnoreId(
          expected,
          explainQueryYaml(
              "source=opensearch-sql_test_index_account | stats count() as c by state | head 10"
                  + " | join type=inner state [ source=opensearch-sql_test_index_account"
                  + " | stats count() as c by state ]"));
    } finally {
      resetQueryBucketSize();
    }
  }

  @Test
  public void testPaginatingAggForHeadFrom() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    try {
      setQueryBucketSize(2);
      String expected = loadExpectedPlan("explain_agg_paginating_head_from.yaml");
      assertYamlEqualsIgnoreId(
          expected,
          explainQueryYaml(
              "source=opensearch-sql_test_index_account | stats count() as c by state | head 10"
                  + " from 2"));
    } finally {
      resetQueryBucketSize();
    }
  }

  @Test
  public void testPaginatingHeadSizeNoLessThanQueryBucketSize() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    try {
      setQueryBucketSize(2);
      String expected =
          loadExpectedPlan("explain_agg_paginating_head_size_query_bucket_size1.yaml");
      assertYamlEqualsIgnoreId(
          expected,
          explainQueryYaml(
              String.format(
                  "source=%s | stats count() by age | sort -age | head 3", TEST_INDEX_BANK)));
      expected = loadExpectedPlan("explain_agg_paginating_head_size_query_bucket_size2.yaml");
      assertYamlEqualsIgnoreId(
          expected,
          explainQueryYaml(
              String.format(
                  "source=%s | stats count() by age | sort -age | head 2", TEST_INDEX_BANK)));
      expected = loadExpectedPlan("explain_agg_paginating_head_size_query_bucket_size3.yaml");
      assertYamlEqualsIgnoreId(
          expected,
          explainQueryYaml(
              String.format(
                  "source=%s | stats count() by age | sort -age | head 1", TEST_INDEX_BANK)));
    } finally {
      resetQueryBucketSize();
    }
  }

  @Test
  public void testExplainSortOnMeasure() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_agg_sort_on_measure1.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | stats bucket_nullable=false count() by"
                + " state | sort `count()`"));
    expected = loadExpectedPlan("explain_agg_sort_on_measure2.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | stats bucket_nullable=false sum(balance)"
                + " as sum by state | sort - sum"));
    // TODO limit should pushdown to non-composite agg
    expected = loadExpectedPlan("explain_agg_sort_on_measure3.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | stats count() as cnt by span(birthdate, 1d) | sort - cnt",
                TEST_INDEX_BANK)));
    expected = loadExpectedPlan("explain_agg_sort_on_measure4.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | stats bucket_nullable=false sum(balance) by span(age, 5) | sort -"
                    + " `sum(balance)`",
                TEST_INDEX_BANK)));
  }

  @Test
  public void testExplainSortOnMeasureWithScript() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_agg_sort_on_measure_script.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | eval new_state = lower(state) | "
                + "stats bucket_nullable=false count() by new_state | sort `count()`"));
  }

  @Test
  public void testExplainSortOnMeasureMultiTerms() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_agg_sort_on_measure_multi_terms.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | stats bucket_nullable=false count() by"
                + " gender, state | sort `count()`"));
  }

  @Test
  public void testExplainSortOnMeasureMultiTermsWithScript() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_agg_sort_on_measure_multi_terms_script.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | eval new_gender = lower(gender), new_state"
                + " = lower(state) | stats bucket_nullable=false count() by new_gender, new_state |"
                + " sort `count()`"));
  }

  @Test
  public void testExplainSortOnMeasureComplex() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_agg_sort_on_measure_complex1.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | stats bucket_nullable=false sum(balance),"
                + " count() as c, dc(employer) by state | sort - c"));
    expected = loadExpectedPlan("explain_agg_sort_on_measure_complex2.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | eval new_state = lower(state) | stats"
                + " bucket_nullable=false sum(balance), count(), dc(employer) as d by gender,"
                + " new_state | sort - d"));
  }

  @Test
  public void testExplainCompositeMultiBucketsAutoDateThenSortOnMeasureNotPushdown()
      throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("agg_composite_multi_terms_autodate_sort_agg_measure_not_push.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | bin timestamp bins=3 | stats bucket_nullable=false avg(value), count()"
                    + " as cnt by category, value, timestamp | sort cnt",
                TEST_INDEX_TIME_DATA)));
  }

  @Test
  public void testExplainCompositeRangeThenSortOnMeasureNotPushdown() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("agg_composite_range_sort_agg_measure_not_push.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | eval value_range = case(value < 7000, 'small'"
                    + " else 'great') | stats bucket_nullable=false avg(value), count() as cnt by"
                    + " value_range, category | sort cnt",
                TEST_INDEX_TIME_DATA)));
  }

  @Test
  public void testExplainCompositeAutoDateThenSortOnMeasureNotPushdown() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("agg_composite_autodate_sort_agg_measure_not_push.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | bin timestamp bins=3 | stats bucket_nullable=false avg(value), count()"
                    + " as cnt by timestamp, category | sort cnt",
                TEST_INDEX_TIME_DATA)));
  }

  @Test
  public void testExplainCompositeRangeAutoDateThenSortOnMeasureNotPushdown() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("agg_composite_autodate_range_metric_sort_agg_measure_not_push.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | bin timestamp bins=3 | eval value_range = case(value < 7000, 'small'"
                    + " else 'great') | stats bucket_nullable=false avg(value), count() as cnt by"
                    + " timestamp, value_range, category | sort cnt",
                TEST_INDEX_TIME_DATA)));
  }

  @Test
  public void testExplainMultipleCollationsWithSortOnOneMeasureNotPushDown() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected =
        loadExpectedPlan("explain_multiple_agg_with_sort_on_one_measure_not_push1.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | stats bucket_nullable=false count() as c,"
                + " sum(balance) as s by state | sort c, state"));
    expected = loadExpectedPlan("explain_multiple_agg_with_sort_on_one_measure_not_push2.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | stats bucket_nullable=false count() as c,"
                + " sum(balance) as s by state | sort c, s"));
  }

  @Test
  public void testExplainSortOnMeasureMultiBucketsNotMultiTermsNotPushDown() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_agg_sort_on_measure_multi_buckets_not_pushed.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | stats bucket_nullable=false count() as c,"
                + " sum(balance) as s by state, span(age, 5) | sort c"));
  }

  @Test
  public void testExplainEvalMax() throws IOException {
    String expected = loadExpectedPlan("explain_eval_max.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | eval new = max(1, 2, 3, age, 'banana')"));
  }

  @Test
  public void testExplainEvalMin() throws IOException {
    String expected = loadExpectedPlan("explain_eval_min.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | eval new = min(1, 2, 3, age, 'banana')"));
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
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("explain_filter_script_ip_push.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | where cidrmatch(host, '0.0.0.0/24') | fields host",
                TEST_INDEX_WEBLOGS)));

    assertYamlEqualsIgnoreId(
        loadExpectedPlan("explain_agg_script_timestamp_push.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | eval t = unix_timestamp(birthdate) | stats count() by t | sort t |"
                    + " head 3",
                TEST_INDEX_BANK)));

    assertYamlEqualsIgnoreId(
        loadExpectedPlan("explain_agg_script_udt_arg_push.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | eval t = date_add(birthdate, interval 1 day) | stats count() by"
                    + " span(t, 1d)",
                TEST_INDEX_BANK)));
  }

  @Test
  public void testFillNullValueSyntaxExplain() throws IOException {
    String expected = loadExpectedPlan("explain_fillnull_value_syntax.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | fields age, balance | fillnull value=0", TEST_INDEX_ACCOUNT)));
  }

  @Test
  public void testJoinWithPushdownSortIntoAgg() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    // PPL_JOIN_SUBSEARCH_MAXOUT!=0 will add limit before sort and then prevent sort push down.
    setJoinSubsearchMaxOut(0);
    String expected = loadExpectedPlan("explain_join_with_agg.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | stats COUNT() by age, gender | join left=L right=R ON L.gender ="
                    + " R.gender [source=%s | stats COUNT() as overall_cnt by gender]",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT)));
    resetJoinSubsearchMaxOut();
  }

  @Test
  public void testReplaceCommandExplain() throws IOException {
    String expected = loadExpectedPlan("explain_replace_command.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | replace 'IL' WITH 'Illinois' IN state | fields state",
                TEST_INDEX_ACCOUNT)));
  }

  @Test
  public void testReplaceCommandWildcardExplain() throws IOException {
    String expected = loadExpectedPlan("explain_replace_wildcard.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | replace '*L' WITH 'STATE_IL' IN state | fields state",
                TEST_INDEX_ACCOUNT)));
  }

  @Test
  public void testExplainRareCommandUseNull() throws IOException {
    String expected = loadExpectedPlan("explain_rare_usenull_false.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format("source=%s | rare 2 usenull=false state by gender", TEST_INDEX_ACCOUNT)));
    expected = loadExpectedPlan("explain_rare_usenull_true.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format("source=%s | rare 2 usenull=true state by gender", TEST_INDEX_ACCOUNT)));
    withSettings(
        Key.PPL_SYNTAX_LEGACY_PREFERRED,
        "false",
        () -> {
          try {
            assertYamlEqualsIgnoreId(
                loadExpectedPlan("explain_rare_usenull_false.yaml"),
                explainQueryYaml(
                    String.format("source=%s | rare 2 state by gender", TEST_INDEX_ACCOUNT)));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  public void testExplainTopCommandUseNull() throws IOException {
    String expected = loadExpectedPlan("explain_top_usenull_false.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format("source=%s | top 2 usenull=false state by gender", TEST_INDEX_ACCOUNT)));
    expected = loadExpectedPlan("explain_top_usenull_true.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format("source=%s | top 2 usenull=true state by gender", TEST_INDEX_ACCOUNT)));
    withSettings(
        Key.PPL_SYNTAX_LEGACY_PREFERRED,
        "false",
        () -> {
          try {
            assertYamlEqualsIgnoreId(
                loadExpectedPlan("explain_top_usenull_false.yaml"),
                explainQueryYaml(
                    String.format("source=%s | top 2 state by gender", TEST_INDEX_ACCOUNT)));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  // Test cases for verifying the fix of https://github.com/opensearch-project/sql/issues/4571
  @Test
  public void testPushDownMinOrMaxAggOnDerivedField() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_min_max_agg_on_derived_field.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | eval balance2 = CEIL(balance/10000.0) "
                    + "| stats MIN(balance2), MAX(balance2)",
                TEST_INDEX_ACCOUNT)));
  }

  @Test
  public void testExplainChartWithSingleGroupKey() throws IOException {
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("chart_single_group_key.yaml"),
        explainQueryYaml(
            String.format("source=%s | chart avg(balance) by gender", TEST_INDEX_BANK)));

    assertYamlEqualsIgnoreId(
        loadExpectedPlan("chart_with_integer_span.yaml"),
        explainQueryYaml(
            String.format("source=%s | chart max(balance) by age span=10", TEST_INDEX_BANK)));

    assertYamlEqualsIgnoreId(
        loadExpectedPlan("chart_with_timestamp_span.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | chart count by @timestamp span=1day", TEST_INDEX_TIME_DATA)));
  }

  @Test
  public void testExplainChartWithMultipleGroupKeys() throws IOException {
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("chart_multiple_group_keys.yaml"),
        explainQueryYaml(
            String.format("source=%s | chart avg(balance) over gender by age", TEST_INDEX_BANK)));

    assertYamlEqualsIgnoreId(
        loadExpectedPlan("chart_timestamp_span_and_category.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | chart max(value) over timestamp span=1week by category",
                TEST_INDEX_TIME_DATA)));
  }

  @Test
  public void testExplainChartWithLimits() throws IOException {
    String expected = loadExpectedPlan("chart_with_limit.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | chart limit=0 avg(balance) over state by gender", TEST_INDEX_BANK)));

    assertYamlEqualsIgnoreId(
        loadExpectedPlan("chart_use_other.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | chart limit=2 useother=true otherstr='max_among_other'"
                    + " max(severityNumber) over flags by severityText",
                TEST_INDEX_OTEL_LOGS)));
  }

  @Test
  public void testExplainChartWithNullStr() throws IOException {
    String expected = loadExpectedPlan("chart_null_str.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | chart limit=10 usenull=true nullstr='nil' avg(balance) over gender by"
                    + " age span=10",
                TEST_INDEX_BANK_WITH_NULL_VALUES)));
  }

  @Test
  public void testCasePushdownAsRangeQueryExplain() throws IOException {
    // CASE 1: Range - Metric
    // 1.1 Range - Metric
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("agg_range_metric_push.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | eval age_range = case(age < 30, 'u30', age < 40, 'u40' else 'u100') |"
                    + " stats avg(age) as avg_age by age_range",
                TEST_INDEX_BANK)));

    // 1.2 Range - Metric (COUNT)
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("agg_range_count_push.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | eval age_range = case(age < 30, 'u30', age >= 30 and age < 40, 'u40'"
                    + " else 'u100') | stats avg(age) by age_range",
                TEST_INDEX_BANK)));

    // 1.3 Range - Range - Metric
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("agg_range_range_metric_push.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | eval age_range = case(age < 30, 'u30', age < 40, 'u40' else 'u100'),"
                    + " balance_range = case(balance < 20000, 'medium' else 'high') | stats"
                    + " avg(balance) as avg_balance by age_range, balance_range",
                TEST_INDEX_BANK)));

    // 1.4 Range - Metric (With null & discontinuous ranges)
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("agg_range_metric_complex_push.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | eval age_range = case(age < 30, 'u30', (age >= 35 and age < 40) or age"
                    + " >= 80, '30-40 or >=80') | stats avg(balance) by age_range",
                TEST_INDEX_BANK)));

    // 1.5 Should not be pushed because the range is not closed-open
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("agg_case_cannot_push.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | eval age_range = case(age < 30, 'u30', age >= 30 and age <= 40, 'u40'"
                    + " else 'u100') | stats avg(age) as avg_age by age_range",
                TEST_INDEX_BANK)));

    // 1.6 Should not be pushed as range query because the result expression is not a string
    // literal.
    // Range aggregation keys must be strings
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("agg_case_num_res_cannot_push.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | eval age_range = case(age < 30, 30 else 100) | stats count() by"
                    + " age_range",
                TEST_INDEX_BANK)));

    // CASE 2: Composite - Range - Metric
    // 2.1 Composite (term) - Range - Metric
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("agg_composite_range_metric_push.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | eval age_range = case(age < 30, 'u30' else 'a30') | stats avg(balance)"
                    + " by state, age_range",
                TEST_INDEX_BANK)));

    // 2.2 Composite (date histogram) - Range - Metric
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("agg_composite_date_range_push.yaml"),
        explainQueryYaml(
            "source=opensearch-sql_test_index_time_data | eval value_range = case(value < 7000,"
                + " 'small' else 'large') | stats avg(value) by value_range, span(@timestamp,"
                + " 1h)"));

    // 2.3 Composite(2 fields) - Range - Metric (with count)
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("agg_composite2_range_count_push.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | eval age_range = case(age < 30, 'u30' else 'a30') | stats"
                    + " avg(balance), count() by age_range, state, gender",
                TEST_INDEX_BANK)));

    // 2.4 Composite (2 fields) - Range - Range - Metric (with count)
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("agg_composite2_range_range_count_push.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | eval age_range = case(age < 35, 'u35' else 'a35'), balance_range ="
                    + " case(balance < 20000, 'medium' else 'high') | stats avg(balance) as"
                    + " avg_balance by age_range, balance_range, state",
                TEST_INDEX_BANK)));

    // 2.5 Should not be pushed down as range query because case result expression is not constant
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("agg_case_composite_cannot_push.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | eval age_range = case(age < 35, 'u35' else email) | stats avg(balance)"
                    + " as avg_balance by age_range, state",
                TEST_INDEX_BANK)));
  }

  @Test
  public void testNestedAggregationsExplain() throws IOException {
    // TODO: Remove after resolving: https://github.com/opensearch-project/sql/issues/4578
    enabledOnlyWhenPushdownIsEnabled();
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("agg_composite_autodate_range_metric_push.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | bin timestamp bins=3 | eval value_range = case(value < 7000, 'small'"
                    + " else 'great') | stats bucket_nullable=false avg(value), count() by"
                    + " timestamp, value_range, category",
                TEST_INDEX_TIME_DATA)));
  }

  @Test
  public void testTopKThenSortExplain() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_top_k_then_sort_push.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account"
                + "| sort balance"
                + "| head 5 "
                + "| sort age "
                + "| fields age"));
  }

  @Test
  public void testGeoIpPushedInAgg() throws IOException {
    // This explain IT verifies that externally registered UDF can be properly pushed down
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("udf_geoip_in_agg_pushed.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | eval info = geoip('my-datasource', host) | stats count() by info.city",
                TEST_INDEX_WEBLOGS)));
  }

  @Test
  public void testInternalItemAccessOnStructs() throws IOException {
    String expected = loadExpectedPlan("access_struct_subfield_with_item.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | eval info = geoip('dummy-datasource', host) | fields host, info,"
                    + " info.dummy_sub_field",
                TEST_INDEX_WEBLOGS)));
  }

  @Test
  public void testaddTotalsExplain() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_add_totals.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account"
                + "| head 5 "
                + "| addtotals  balance age label='ColTotal' "
                + " fieldname='CustomSum' labelfield='all_emp_total' row=true col=true"));
  }

  @Test
  public void testaddColTotalsExplain() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_add_col_totals.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account"
                + "| head 5 "
                + "|  addcoltotals balance age label='GrandTotal'"));
  }

  public void testComplexDedup() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_dedup_complex1.yaml");
    assertYamlEqualsIgnoreId(
        expected, explainQueryYaml("source=opensearch-sql_test_index_account | dedup 1 gender"));
    expected = loadExpectedPlan("explain_dedup_complex2.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | fields account_number, gender, age, state |"
                + " dedup 1 gender, state"));
    expected = loadExpectedPlan("explain_dedup_complex3.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml("source=opensearch-sql_test_index_account | dedup 2 gender, state"));
    expected = loadExpectedPlan("explain_dedup_complex4.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | fields account_number, gender, age, state |"
                + " dedup 2 gender, state"));
  }

  @Test
  public void testDedupExpr() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_dedup_expr1.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | eval new_gender = lower(gender) | dedup 1"
                + " new_gender"));
    expected = loadExpectedPlan("explain_dedup_expr2.yaml");
    String alternative = loadExpectedPlan("explain_dedup_expr2_alternative.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        alternative,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | fields account_number, gender, age, state |"
                + " eval new_gender = lower(gender), new_state = lower(state) | dedup 1 new_gender,"
                + " new_state"));
    expected = loadExpectedPlan("explain_dedup_expr3.yaml");
    alternative = loadExpectedPlan("explain_dedup_expr3_alternative.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        alternative,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | eval new_gender = lower(gender) | eval"
                + " new_state = lower(state) | dedup 2 new_gender, new_state"));
    expected = loadExpectedPlan("explain_dedup_expr4.yaml");
    alternative = loadExpectedPlan("explain_dedup_expr4_alternative.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        alternative,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | fields account_number, gender, age, state |"
                + " eval new_gender = lower(gender) | eval new_state = lower(state) | sort gender,"
                + " -state | dedup 2 new_gender, new_state"));
  }

  @Test
  public void testDedupRename() throws IOException {
    // rename changes nothing, reuse the same yaml files
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_dedup_expr1.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | eval tmp_gender = lower(gender) | rename"
                + " tmp_gender as new_gender | dedup 1 new_gender"));
    expected = loadExpectedPlan("explain_dedup_expr2.yaml");
    String alternative = loadExpectedPlan("explain_dedup_expr2_alternative.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        alternative,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | fields account_number, gender, age, state |"
                + " eval tmp_gender = lower(gender), tmp_state = lower(state) | rename tmp_gender"
                + " as new_gender | rename tmp_state as new_state | dedup 1 new_gender,"
                + " new_state"));
    expected = loadExpectedPlan("explain_dedup_expr3.yaml");
    alternative = loadExpectedPlan("explain_dedup_expr3_alternative.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        alternative,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | eval tmp_gender = lower(gender) | eval"
                + " tmp_state = lower(state) | rename tmp_gender as new_gender | rename tmp_state"
                + " as new_state | dedup 2 new_gender, new_state"));
    expected = loadExpectedPlan("explain_dedup_expr4.yaml");
    alternative = loadExpectedPlan("explain_dedup_expr4_alternative.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        alternative,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | fields account_number, gender, age, state |"
                + " eval tmp_gender = lower(gender) | eval tmp_state = lower(state) | rename"
                + " tmp_gender as new_gender | rename tmp_state as new_state | sort gender,"
                + " -state | dedup 2 new_gender, new_state"));
  }

  @Test
  public void testRenameDedupThenSortExpr() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_dedup_expr_complex1.yaml");
    String alternative = loadExpectedPlan("explain_dedup_expr_complex1_alternative.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        alternative,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | fields account_number, gender, age, state |"
                + " eval tmp_gender = lower(gender) | eval tmp_state = lower(state) | rename"
                + " tmp_gender as new_gender | rename tmp_state as new_state | sort new_gender,"
                + " -new_state | dedup 2 new_gender, new_state"));
    expected = loadExpectedPlan("explain_dedup_expr_complex2.yaml");
    alternative = loadExpectedPlan("explain_dedup_expr_complex2_alternative.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        alternative,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | fields account_number, gender, age, state |"
                + " eval tmp_gender = lower(gender) | eval tmp_state = lower(state) | rename"
                + " tmp_gender as new_gender | rename tmp_state as new_state | dedup 2 new_gender,"
                + " new_state | sort new_gender, -new_state"));
  }

  @Test
  public void testDedupWithExpr() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_dedup_with_expr1.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | eval new_gender = lower(gender) | dedup 1"
                + " age"));
    expected = loadExpectedPlan("explain_dedup_with_expr2.yaml");
    String alternative = loadExpectedPlan("explain_dedup_with_expr2_alternative.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        alternative,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | fields account_number, gender, age, state |"
                + " eval new_gender = lower(gender), new_state = lower(state) | dedup 1 age,"
                + " new_state"));
    expected = loadExpectedPlan("explain_dedup_with_expr3.yaml");
    alternative = loadExpectedPlan("explain_dedup_with_expr3_alternative.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        alternative,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | eval new_gender = lower(gender) | eval"
                + " new_state = lower(state) | dedup 2 age, account_number"));
    expected = loadExpectedPlan("explain_dedup_with_expr4.yaml");
    alternative = loadExpectedPlan("explain_dedup_with_expr4_alternative.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        alternative,
        explainQueryYaml(
            "source=opensearch-sql_test_index_account | fields account_number, gender, age, state |"
                + " eval new_gender = lower(gender) | eval new_state = lower(state) | sort gender,"
                + " -state | dedup 2 gender, state"));
  }

  @Test
  public void testDedupTextTypeNotPushdown() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_dedup_text_type_no_push.yaml");
    assertYamlEqualsIgnoreId(
        expected, explainQueryYaml(String.format("source=%s | dedup email", TEST_INDEX_BANK)));
  }

  @Test
  public void testAliasTypeField() throws IOException {
    String expected = loadExpectedPlan("explain_alias_type_field.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            String.format(
                "source=%s | fields alias_col | where alias_col > 10 | stats avg(alias_col)",
                TEST_INDEX_ALIAS)));
  }

  @Test
  public void testRexStandardizationForScript() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("explain_extended_for_standardization.yaml"),
        explainQueryYaml(
            String.format(
                "source=%s | eval age_range = case(age < 30, 'u30', age >= 30 and age <= 40, 'u40'"
                    + " else 'u100') | stats avg(age) as avg_age by age_range",
                TEST_INDEX_BANK),
            ExplainMode.EXTENDED));
  }

  @Test
  public void testNestedAggPushDownExplain() throws Exception {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_nested_agg_push.yaml");

    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_nested_simple | stats count(address.area) as"
                + " count_area, min(address.area) as min_area, max(address.area) as max_area,"
                + " avg(address.area) as avg_area, avg(age) as avg_age by name"));
  }

  @Test
  public void testNestedSingleCountPushDownExplain() throws Exception {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_nested_agg_single_count_push.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_nested_simple | stats count(address.area)"));
  }

  @Test
  public void testNestedAggPushDownSortExplain() throws Exception {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_nested_agg_sort_push.yaml");

    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_nested_simple | stats count() by address.city | sort"
                + " -address.city"));
  }

  @Test
  public void testNestedAggByPushDownExplain() throws Exception {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_nested_agg_by_push.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_nested_simple | stats min(address.area) by"
                + " address.city"));
    // Whatever bucket_nullable=false or bucket_nullable=true is, the plans should be the same.
    // The filter(is_not_null) can be safe removed since nested agg only works when pushdown is
    // applied.
    expected = loadExpectedPlan("explain_nested_agg_by_bucket_nullable_push.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_nested_simple | stats bucket_nullable=false"
                + " min(address.area) by address.city"));
  }

  @Test
  public void testNestedAggTop() throws Exception {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_nested_agg_top_push.yaml");

    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_nested_simple | top usenull=false address.city"));
  }

  @Test
  public void testNestedAggDedupNotPushed() throws Exception {
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_nested_agg_dedup_not_push.yaml");

    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml("source=opensearch-sql_test_index_nested_simple | dedup address.city"));
  }

  @Test
  public void testNestedAggExplainWhenPushdownNotApplied() throws Exception {
    enabledOnlyWhenPushdownIsEnabled();
    Throwable e =
        assertThrowsWithReplace(
            UnsupportedOperationException.class,
            () ->
                explainQueryYaml(
                    "source=opensearch-sql_test_index_nested_simple | head 10000 | stats"
                        + " count(address.area) as count_area, min(address.area) as min_area,"
                        + " max(address.area) as max_area, avg(address.area) as avg_area, avg(age)"
                        + " as avg_age by name"));
    verifyErrorMessageContains(e, "Cannot execute nested aggregation on");
  }

  @Test
  public void testNotBetweenPushDownExplain() throws Exception {
    // test for issue https://github.com/opensearch-project/sql/issues/4903
    enabledOnlyWhenPushdownIsEnabled();
    String expected = loadExpectedPlan("explain_not_between_push.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(
            "source=opensearch-sql_test_index_bank | where age not between 30 and 39"));
  }

  @Test
  public void testSpathWithoutPathExplain() throws IOException {
    String expected = loadExpectedPlan("explain_spath_without_path.yaml");
    assertYamlEqualsIgnoreId(
        expected, explainQueryYaml(source(TEST_INDEX_LOGS, "spath input=message | fields test")));
  }

  @Test
  public void testSpathWithDynamicFieldsExplain() throws IOException {
    String expected = loadExpectedPlan("explain_spath_with_dynamic_fields.yaml");
    assertYamlEqualsIgnoreId(
        expected,
        explainQueryYaml(source(TEST_INDEX_LOGS, "spath input=message | where status = '200'")));
  }

  @Test
  public void testExplainInVariousModeAndFormat() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String query =
        "source=opensearch-sql_test_index_account"
            + "| where age > 30 "
            + "| stats avg(age) AS avg_age by state, city "
            + "| sort state "
            + "| fields - city "
            + "| eval age2 = avg_age + 2 "
            + "| dedup age2 "
            + "| fields age2";
    ExplainMode[] explainModes =
        new ExplainMode[] {
          ExplainMode.SIMPLE, ExplainMode.STANDARD, ExplainMode.EXTENDED, ExplainMode.COST
        };
    for (ExplainMode explainMode : explainModes) {
      String modeName = explainMode.getModeName().toLowerCase(Locale.ROOT);
      assertYamlEqualsIgnoreId(
          loadExpectedPlan(String.format("explain_output_%s.yaml", modeName)),
          explainQueryYaml(query, explainMode));
      assertJsonEqualsIgnoreId(
          loadExpectedPlan(String.format("explain_output_%s.json", modeName)),
          explainQueryToString(query, explainMode));
    }
  }

  @Test
  public void testExplainBWC() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    String query =
        "source=opensearch-sql_test_index_account"
            + "| where age > 30 "
            + "| stats avg(age) AS avg_age by state, city "
            + "| sort state "
            + "| fields - city "
            + "| eval age2 = avg_age + 2 "
            + "| dedup age2 "
            + "| fields age2";
    Format[] formats = new Format[] {Format.SIMPLE, Format.STANDARD, Format.EXTENDED, Format.COST};
    for (Format format : formats) {
      String formatName = format.getFormatName().toLowerCase(Locale.ROOT);
      assertJsonEqualsIgnoreId(
          loadExpectedPlan(String.format("explain_output_%s.json", formatName)),
          explainQueryToStringBWC(query, format));
    }
  }

  @Test
  public void testFilterOnComputedNestedFields() throws IOException {
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("filter_computed_nested.yaml"),
        explainQueryYaml(
            StringUtils.format(
                "source=%s | eval proj_name_len=length(projects.name) | fields projects.name,"
                    + " proj_name_len | where proj_name_len > 29",
                TEST_INDEX_DEEP_NESTED)));
  }

  @Test
  public void testFilterOnNestedAndRootFields() throws IOException {
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("filter_root_and_nested.yaml"),
        // city is not in a nested object
        explainQueryYaml(
            StringUtils.format(
                "source=%s | where city.name = 'Seattle' and length(projects.name) > 29",
                TEST_INDEX_DEEP_NESTED)));
  }

  @Test
  public void testFilterOnNestedFields() throws IOException {
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("filter_nested_term.yaml"),
        // address is a nested object
        explainQueryYaml(
            StringUtils.format(
                "source=%s | where address.city = 'New york city'", TEST_INDEX_NESTED_SIMPLE)));

    assertYamlEqualsIgnoreId(
        loadExpectedPlan("filter_nested_terms.yaml"),
        explainQueryYaml(
            StringUtils.format(
                "source=%s | where address.city in ('Miami', 'san diego')",
                TEST_INDEX_NESTED_SIMPLE)));
  }

  @Test
  public void testFilterOnMultipleCascadedNestedFields() throws IOException {
    // 1. Access two different hierarchies of nested fields, one at author.books.reviews, another at
    // author.books
    // 2. One is pushed as nested range query, another is pushed as nested filter query.
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("filter_multiple_nested_cascaded_range.yaml"),
        explainQueryYaml(
            StringUtils.format(
                "source=%s | where author.books.reviews.rating >=4 and author.books.reviews.rating"
                    + " < 6 and author.books.title = 'The Shining'",
                TEST_INDEX_CASCADED_NESTED)));
  }

  @Test
  public void testAggFilterOnNestedFields() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    assertYamlEqualsIgnoreId(
        loadExpectedPlan("agg_filter_nested.yaml"),
        explainQueryYaml(
            StringUtils.format(
                "source=%s | stats count(eval(author.name < 'K')) as george_and_jk",
                TEST_INDEX_CASCADED_NESTED)));
  }
}
