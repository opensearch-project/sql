/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.junit.Ignore;
import org.opensearch.sql.ppl.StatsCommandIT;

// TODO
@Ignore("Not all agg functions are supported in Calcite now")
public class CalciteStatsCommandIT extends StatsCommandIT {
  @Override
  public void init() throws IOException {
    enableCalcite();
    disallowCalciteFallback();
    super.init();
  }
}
