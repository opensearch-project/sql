/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.fallback;

import java.io.IOException;
import org.junit.Ignore;
import org.opensearch.sql.ppl.DateTimeFunctionIT;

public class CalciteDateTimeFunctionIT extends DateTimeFunctionIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Ignore("https://github.com/opensearch-project/sql/issues/3475")
  @Override
  public void testTimestampDiff() throws IOException {
    super.testTimestampDiff();
  }
}
