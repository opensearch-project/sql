/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.sql.util.MatcherUtils.assertJsonEqualsIgnoreId;

import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.TestUtils;

public class ExplainIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testExplain() throws Exception {
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
  public void testFilterPushDownExplain() throws Exception {
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
  public void testFilterAndAggPushDownExplain() throws Exception {
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
  public void testSortPushDownExplain() throws Exception {
    // TODO fix after https://github.com/opensearch-project/sql/issues/3380
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
  public void testLimitPushDownExplain() throws Exception {
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
  public void testLimitWithFilterPushdownExplain() throws Exception {
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
  public void testMultipleLimitExplain() throws Exception {
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
  public void testLimitWithMultipleOffsetPushdownExplain() throws Exception {
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
  public void testFillNullPushDownExplain() throws Exception {
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
  public void testTrendlinePushDownExplain() throws Exception {
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
  public void testTrendlineWithSortPushDownExplain() throws Exception {
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_trendline_sort_push.json")
            : loadFromFile("expectedOutput/ppl/explain_trendline_sort_push.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 5 "
                + "| trendline sort age sma(2, age) as ageTrend "
                + "| fields ageTrend"));
  }

  String loadFromFile(String filename) throws Exception {
    URI uri = Resources.getResource(filename).toURI();
    return new String(Files.readAllBytes(Paths.get(uri)));
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
  public void testPatternsWithoutAggExplain() throws Exception {
    // TODO: Correct calcite expected result once pushdown is supported
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_patterns.json")
            : loadFromFile("expectedOutput/ppl/explain_patterns.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString("source=opensearch-sql_test_index_account | patterns email"));
  }

  @Test
  public void testPatternsWithAggPushDownExplain() throws Exception {
    // TODO: Correct calcite expected result once pushdown is supported
    String expected =
        isCalciteEnabled()
            ? loadFromFile("expectedOutput/calcite/explain_patterns_agg_push.json")
            : loadFromFile("expectedOutput/ppl/explain_patterns_agg_push.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| patterns email "
                + "| stats count() by patterns_field"));
  }
}
