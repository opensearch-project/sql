/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.opensearch.sql.ppl.DedupCommandIT;

public class CalciteDedupCommandIT extends DedupCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }

  @Override
  public void testConsecutiveDedup() throws IOException {
    withFallbackEnabled(
        () -> {
          try {
            super.testConsecutiveDedup();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        "https://github.com/opensearch-project/sql/issues/3415");
  }
}
