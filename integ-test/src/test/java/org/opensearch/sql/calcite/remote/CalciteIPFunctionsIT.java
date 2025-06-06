/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.opensearch.sql.ppl.IPFunctionsIT;

public class CalciteIPFunctionsIT extends IPFunctionsIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }

  @Override
  public void test_cidrmatch() throws IOException {
    withFallbackEnabled(
        () -> {
          try {
            super.test_cidrmatch();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        "https://github.com/opensearch-project/sql/issues/3505");
  }
}
