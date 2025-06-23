/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

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
  }

  @Override
  @Ignore("test only in v2")
  public void testExplainModeUnsupportedInV2() throws IOException {}

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
}
