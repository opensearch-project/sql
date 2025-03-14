/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.junit.Ignore;
import org.opensearch.sql.calcite.standalone.CalcitePPLAggregationIT;
import org.opensearch.sql.ppl.StatsCommandIT;

public class CalciteStatsCommandIT extends StatsCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
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

  @Ignore("https://github.com/opensearch-project/sql/issues/3354")
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
