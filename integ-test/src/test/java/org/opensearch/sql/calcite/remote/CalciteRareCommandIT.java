/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.junit.Ignore;
import org.opensearch.sql.ppl.RareCommandIT;

@Ignore
public class CalciteRareCommandIT extends RareCommandIT {
  @Override
  public void init() throws IOException {
    enableCalcite();
    disallowCalciteFallback();
    super.init();
  }
}
