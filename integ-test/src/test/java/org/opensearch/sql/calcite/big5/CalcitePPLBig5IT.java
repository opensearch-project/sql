/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.big5;

import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.JVM)
public class CalcitePPLBig5IT extends PPLBig5IT {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }
}
