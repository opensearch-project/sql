/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.fallback;

import java.io.IOException;
import org.junit.Ignore;
import org.opensearch.sql.ppl.StatsCommandIT;

public class CalciteStatsCommandIT extends StatsCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Ignore("https://github.com/opensearch-project/sql/issues/3495")
  @Override
  public void testStatsPercentileByNullValue() throws IOException {
    super.testStatsPercentileByNullValue();
  }
}
