/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.opensearch.sql.ppl.ObjectFieldOperateIT;

public class CalciteObjectFieldOperatorIT extends ObjectFieldOperateIT {
  @Override
  public void init() throws IOException {
    enableCalcite();
    disallowCalciteFallback();
    super.init();
  }
}
