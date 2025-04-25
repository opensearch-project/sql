/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.opensearch.sql.ppl.NewAddedCommandsIT;

public class CalciteNewAddedCommandsIT extends NewAddedCommandsIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }
}
