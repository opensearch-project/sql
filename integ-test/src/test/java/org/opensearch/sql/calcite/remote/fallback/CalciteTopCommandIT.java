/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.fallback;

import org.opensearch.sql.ppl.TopCommandIT;

public class CalciteTopCommandIT extends TopCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }
}
