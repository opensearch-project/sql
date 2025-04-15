/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.opensearch.sql.ppl.DataTypeIT;

public class CalciteDataTypeIT extends DataTypeIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }

  @Override
  public void test_nonnumeric_data_types() throws IOException {
    withFallbackEnabled(
        () -> {
          try {
            super.test_nonnumeric_data_types();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        "ignore this class since IP type is unsupported in calcite engine");
  }
}
