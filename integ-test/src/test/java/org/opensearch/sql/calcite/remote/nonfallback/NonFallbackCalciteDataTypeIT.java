/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import org.opensearch.sql.calcite.remote.fallback.CalciteDataTypeIT;

public class NonFallbackCalciteDataTypeIT extends CalciteDataTypeIT {
  @Override
  public void init() throws Exception {
    super.init();
    disallowCalciteFallback();
  }
}
