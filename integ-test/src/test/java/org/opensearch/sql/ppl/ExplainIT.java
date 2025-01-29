/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CITIES;
import static org.opensearch.sql.util.MatcherUtils.assertJsonEquals;

import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.utils.StringUtils;

public class ExplainIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.CITIES);
  }

  @Test
  public void testExplain() throws Exception {
    String expected = loadFromFile("expectedOutput/ppl/explain_output.json");
    assertJsonEquals(
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
    String expected = loadFromFile("expectedOutput/ppl/explain_filter_push.json");

    assertJsonEquals(
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
    String expected = loadFromFile("expectedOutput/ppl/explain_filter_agg_push.json");

    assertJsonEquals(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| where age > 30 "
                + "| stats avg(age) AS avg_age by state, city"));
  }

  @Test
  public void testSortPushDownExplain() throws Exception {
    String expected = loadFromFile("expectedOutput/ppl/explain_sort_push.json");

    assertJsonEquals(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| sort age "
                + "| where age > 30"
                + "| fields age"));
  }

  @Test
  public void testLimitPushDownExplain() throws Exception {
    String expected = loadFromFile("expectedOutput/ppl/explain_limit_push.json");

    assertJsonEquals(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| eval ageMinus = age - 30 "
                + "| head 5 "
                + "| fields ageMinus"));
  }

  @Test
  public void testFillNullPushDownExplain() throws Exception {
    String expected = loadFromFile("expectedOutput/ppl/explain_fillnull_push.json");

    assertJsonEquals(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + " | fillnull with -1 in age,balance | fields age, balance"));
  }

  @Test
  public void testTrendlinePushDownExplain() throws Exception {
    String expected = loadFromFile("expectedOutput/ppl/explain_trendline_push.json");

    assertJsonEquals(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 5 "
                + "| trendline sma(2, age) as ageTrend "
                + "| fields ageTrend"));
  }

  @Test
  public void testTrendlineWithSortPushDownExplain() throws Exception {
    String expected = loadFromFile("expectedOutput/ppl/explain_trendline_sort_push.json");

    assertJsonEquals(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_account"
                + "| head 5 "
                + "| trendline sort age sma(2, age) as ageTrend "
                + "| fields ageTrend"));
  }

  @Test
  public void testFlatten() throws Exception {
    String query = StringUtils.format("source=%s | flatten location", TEST_INDEX_CITIES);
    String actual = explainQueryToString(query);
    String expected = loadFromFile("expectedOutput/ppl/explain_flatten.json");
    assertJsonEquals(expected, actual);
  }

  private static String loadFromFile(String filename)
      throws java.net.URISyntaxException, IOException {
    URI uri = Resources.getResource(filename).toURI();
    return new String(Files.readAllBytes(Paths.get(uri)));
  }
}
