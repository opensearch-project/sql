/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import org.junit.Ignore;
import org.opensearch.sql.calcite.remote.fallback.CalciteDateTimeComparisonIT;

public class NonFallbackCalciteDateTimeComparisonIT extends CalciteDateTimeComparisonIT {

  public NonFallbackCalciteDateTimeComparisonIT(
      String functionCall, String name, Boolean expectedResult) {
    super(functionCall, name, expectedResult);
  }

  @Override
  public void init() throws Exception {
    super.init();
    disallowCalciteFallback();
  }
}
