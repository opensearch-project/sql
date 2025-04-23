/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.opensearch.sql.ppl.OperatorIT;

public class CalciteOperatorIT extends OperatorIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }

  @Override
  public void testModuleOperator() throws IOException {
    super.testModuleOperator();
    withFallbackEnabled(
        () -> {
          try {
            super.testModuleOperator();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        "https://github.com/opensearch-project/sql/issues/3398");
  }
}
