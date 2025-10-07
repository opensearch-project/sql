/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.opensearch.sql.ppl.StatsCommandIT;

public class CalciteStatsCommandIT extends StatsCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    setQuerySizeLimit(2000);
  }
}
