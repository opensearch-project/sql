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
  public void testTrendlinePushDownExplain() throws Exception {
    withFallbackEnabled(
        () -> {
          try {
            super.testTrendlinePushDownExplain();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        "https://github.com/opensearch-project/sql/issues/3466");
  }

  @Override
  public void testTrendlineWithSortPushDownExplain() throws Exception {
    withFallbackEnabled(
        () -> {
          try {
            super.testTrendlineWithSortPushDownExplain();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        "https://github.com/opensearch-project/sql/issues/3466");
  }

  @Test
  public void testNestedAggPushDownExplain() throws Exception {
    String expected = loadFromFile("expectedOutput/calcite/explain_nested_agg_push.json");

    assertJsonEqualsIgnoreId(
        expected,
        explainQueryToString(
            "source=opensearch-sql_test_index_nested_simple| stats count(address.area) as"
                + " count_area, min(address.area) as min_area, max(address.area) as max_area,"
                + " avg(address.area) as avg_area, avg(age) as avg_age by name"));
  }

  @Override
  @Ignore("test only in v2")
  public void testExplainModeUnsupportedInV2() throws IOException {}
}
