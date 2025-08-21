/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.assertJsonEqualsIgnoreId;

import java.io.IOException;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.ppl.ExplainIT;

public class CalciteExplainIT extends ExplainIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
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
  public void noPushDownForAggOnWindow() throws IOException {
    Assume.assumeTrue("This test is only for push down enabled", isPushdownEnabled());
    String query =
        "source=opensearch-sql_test_index_account | patterns address method=BRAIN  | stats count()"
            + " by patterns_field";
    var result = explainQueryToString(query);
    String expected = loadFromFile("expectedOutput/calcite/explain_agg_on_window.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  // Only for Calcite - VALUES function explain tests
  @Test
  public void testExplainValuesFunction() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | where account_number < 10 | stats"
            + " values(gender) as unique_genders";
    var result = explainQueryToString(query);

    // Verify that the plan contains proper aggregation with VALUES function
    assertTrue("Plan should contain LogicalAggregate", result.contains("LogicalAggregate"));
    assertTrue("Plan should contain VALUES function", result.contains("VALUES"));
  }

  // Only for Calcite - LIST function explain tests
  @Test
  public void testExplainListFunction() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | where account_number < 10 | stats"
            + " list(firstname) as all_names";
    var result = explainQueryToString(query);

    // Verify that the plan contains proper aggregation with LIST function
    assertTrue("Plan should contain LogicalAggregate", result.contains("LogicalAggregate"));
    assertTrue("Plan should contain LIST function", result.contains("LIST"));
  }

  // Only for Calcite - Combined VALUES and LIST functions explain tests
  @Test
  public void testExplainValuesWithListFunction() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | where account_number < 10 | stats"
            + " values(gender) as unique_genders, list(firstname) as all_names";
    var result = explainQueryToString(query);

    // Verify that the plan contains proper aggregation with both functions
    assertTrue("Plan should contain LogicalAggregate", result.contains("LogicalAggregate"));
    assertTrue("Plan should contain VALUES function", result.contains("VALUES"));
    assertTrue("Plan should contain LIST function", result.contains("LIST"));
  }

  // Only for Calcite - Multivalue stats functions with other aggregates
  @Test
  public void testExplainMultiValueStatsFunctions() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | where account_number < 20 | stats count() as"
            + " total_count, values(state) as unique_states, list(lastname) as all_lastnames by"
            + " employer";
    var result = explainQueryToString(query);

    // Verify that the plan contains proper aggregation with mixed functions
    assertTrue("Plan should contain LogicalAggregate", result.contains("LogicalAggregate"));
    assertTrue("Plan should contain COUNT function", result.contains("COUNT"));
    assertTrue("Plan should contain VALUES function", result.contains("VALUES"));
    assertTrue("Plan should contain LIST function", result.contains("LIST"));
    assertTrue(
        "Plan should contain group by employer",
        result.contains("employer") || result.contains("group="));
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
