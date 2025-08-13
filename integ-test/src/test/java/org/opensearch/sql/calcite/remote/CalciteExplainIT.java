/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

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
    disallowCalciteFallback();
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
}
