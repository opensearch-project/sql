/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

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

  @Ignore("Percentile is unsupported in Calcite now")
  @Override
  public void testStatsPercentile() throws IOException {
    super.testStatsPercentile();
  }

  @Ignore("Percentile is unsupported in Calcite now")
  @Override
  public void testStatsPercentileWithNull() throws IOException {
    super.testStatsPercentileWithNull();
  }

  @Ignore("Percentile is unsupported in Calcite now")
  @Override
  public void testStatsPercentileWithCompression() throws IOException {
    super.testStatsPercentileWithCompression();
  }

  @Ignore("Percentile is unsupported in Calcite now")
  @Override
  public void testStatsPercentileWhere() throws IOException {
    super.testStatsPercentileWhere();
  }

  @Ignore("Percentile is unsupported in Calcite now")
  @Override
  public void testStatsPercentileByNullValue() throws IOException {
    super.testStatsPercentileByNullValue();
  }

  @Ignore("Percentile is unsupported in Calcite now")
  @Override
  public void testStatsPercentileBySpan() throws IOException {
    super.testStatsPercentileBySpan();
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
