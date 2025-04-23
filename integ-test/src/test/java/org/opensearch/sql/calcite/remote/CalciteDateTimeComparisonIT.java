/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.opensearch.sql.ppl.DateTimeComparisonIT;

public class CalciteDateTimeComparisonIT extends DateTimeComparisonIT {

  public CalciteDateTimeComparisonIT(String functionCall, String name, Boolean expectedResult) {
    super(functionCall, name, expectedResult);
  }

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }
}
