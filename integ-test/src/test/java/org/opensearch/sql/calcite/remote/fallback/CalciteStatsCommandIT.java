/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.fallback;

import org.junit.Ignore;
import org.opensearch.sql.ppl.StatsCommandIT;

import java.io.IOException;

public class CalciteStatsCommandIT extends StatsCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Ignore("https://github.com/opensearch-project/sql/issues/3475")
  @Override
  public void testStatsPercentileByNullValue() throws IOException {
    super.testStatsPercentileByNullValue();
  }

}
