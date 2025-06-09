/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.junit.Ignore;
import org.opensearch.sql.ppl.MathematicalFunctionIT;

public class CalciteMathematicalFunctionIT extends MathematicalFunctionIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }
}
