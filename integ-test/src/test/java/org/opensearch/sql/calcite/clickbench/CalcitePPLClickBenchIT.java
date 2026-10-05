/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.clickbench;

import static org.opensearch.sql.util.Capability.LUCENE_PUSHDOWN_EXPLAIN;

import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.opensearch.sql.util.RequiresCapability;

@FixMethodOrder(MethodSorters.JVM)
@RequiresCapability(LUCENE_PUSHDOWN_EXPLAIN)
public class CalcitePPLClickBenchIT extends PPLClickBenchIT {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }
}
