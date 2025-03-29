/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.junit.Ignore;
import org.opensearch.sql.calcite.remote.fallback.CalciteStatsCommandIT;
import org.opensearch.sql.calcite.standalone.CalcitePPLAggregationIT;

public class NonFallbackCalciteStatsCommandIT extends CalciteStatsCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    disallowCalciteFallback();
  }

  @Ignore("Percentile issue")
  @Override
  public void testStatsPercentileByNullValue() throws IOException {
    super.testStatsPercentileByNullValue();
  }

  @Override
  public void testStatsTimeSpan() throws IOException {
    super.testStatsTimeSpan();
  }

  /**
   * the super class returns rows(1, 20, "f", "VA") but in Calcite implementation, it returns
   * rows(1, 20, "F", "VA") check {@link CalcitePPLAggregationIT#testStatsByMultipleFieldsAndSpan()}
   * and {@link CalcitePPLAggregationIT#testStatsBySpanAndMultipleFields()}
   */
  @Override
  public void testStatsByMultipleFieldsAndSpan() throws IOException {}

  @Override
  public void testStatsBySpanAndMultipleFields() throws IOException {}
}
