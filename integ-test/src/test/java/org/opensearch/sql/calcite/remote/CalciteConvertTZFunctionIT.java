/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.opensearch.sql.ppl.ConvertTZFunctionIT;

public class CalciteConvertTZFunctionIT extends ConvertTZFunctionIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    datatype = "timestamp";
    disallowCalciteFallback();
  }
}
