/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_HOBBIES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_OCCUPATION;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.util.MatcherUtils.assertJsonEqualsIgnoreId;

import java.io.IOException;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.ExplainIT;

public class CalciteExplainIT extends ExplainIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.OCCUPATION);
    loadIndex(Index.HOBBIES);
  }

  @Test
  public void testPushDownSystemLimitForJoinExplain() throws Exception {
    // the SYSTEM LIMIT should apply to each table of join
    String expected = loadFromFile("expectedOutput/calcite/explain_join_push_system_limit.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | inner join left=a, right=b ON a.name = b.name %s | fields "
                    + "a.name, a.age, a.state, a.country, b.occupation, b.country, b.salary",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION)));
  }

  @Test
  public void testPushDownSystemLimitForMultipleJoinExplain() throws Exception {
    // the SYSTEM LIMIT should apply to each table of multi-join
    String expected =
        loadFromFile("expectedOutput/calcite/explain_multi_join_push_system_limit.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source=%s | inner join left=a, right=b ON a.name = b.name %s"
                    + " | left join left=a, right=b ON a.name = b.name %s",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION, TEST_INDEX_HOBBIES)));
  }

  @Test
  public void testPushDownSystemLimitForExistsSubqueryExplain() throws Exception {
    // the SYSTEM LIMIT should apply to each table of exists subquery
    // since exists subquery translates to hash join
    String expected =
        loadFromFile("expectedOutput/calcite/explain_exists_subquery_push_system_limit.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source = %s exists [ source = %s | where name = %s.name ]",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION, TEST_INDEX_STATE_COUNTRY)));
  }

  @Test
  public void testPushDownSystemLimitForInSubqueryExplain() throws Exception {
    // the SYSTEM LIMIT should apply to each table of in subquery
    // since in subquery translates to hash join
    String expected =
        loadFromFile("expectedOutput/calcite/explain_in_subquery_push_system_limit.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            String.format(
                "source = %s | where name in [ source = %s | fields name ]",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION)));
  }

  @Override
  @Ignore("test only in v2")
  public void testExplainModeUnsupportedInV2() throws IOException {}
}
