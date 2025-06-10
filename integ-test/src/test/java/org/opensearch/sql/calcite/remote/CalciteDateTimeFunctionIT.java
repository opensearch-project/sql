/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.junit.Ignore;
import org.opensearch.sql.ppl.DateTimeFunctionIT;

public class CalciteDateTimeFunctionIT extends DateTimeFunctionIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }

  // TODO: Remove this when supporting type coercion and casting with Calcite
  @Ignore
  @Override
  public void testUnixTimestampWithTimestampString() throws IOException {}
}
