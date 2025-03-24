/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.fallback;

import org.opensearch.sql.ppl.DateTimeFunctionIT;

public class CalciteDateTimeFunctionIT extends DateTimeFunctionIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }
}
