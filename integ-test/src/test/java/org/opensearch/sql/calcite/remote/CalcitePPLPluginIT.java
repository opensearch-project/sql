/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.opensearch.sql.ppl.PPLPluginIT;

public class CalcitePPLPluginIT extends PPLPluginIT {
  @Override
  public void init() throws Exception {
    enableCalcite();
    disallowCalciteFallback();
    super.init();
  }
}
