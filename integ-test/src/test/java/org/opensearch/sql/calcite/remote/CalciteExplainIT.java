/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.assertJsonEqualsIgnoreId;

import java.io.IOException;
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
    String expected =
        loadFromFile("expectedOutput/calcite/explain_sarg_filter_push_single_range.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  // Only for Calcite
  @Test
  public void supportSearchSargPushDown_multiRange() throws IOException {
    String query =
        "source=opensearch-sql_test_index_account | where (age > 20 and age < 28) or (age > 25 and"
            + " age < 30) or (age >= 1 and age <= 10) or age = 0  | fields age";
    var result = explainQueryToString(query);
    String expected =
        loadFromFile("expectedOutput/calcite/explain_sarg_filter_push_multi_range.json");
    assertJsonEqualsIgnoreId(expected, result);
  }

  // Only for Calcite
  @Test
  public void supportSearchSargPushDown_timeRange() throws IOException {
    String expected =
        loadFromFile("expectedOutput/calcite/explain_sarg_filter_push_time_range.json");
    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_bank"
                + "| where birthdate >= '2016-12-08 00:00:00.000000000' "
                + "and birthdate < '2018-11-09 00:00:00.000000000' "));
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
