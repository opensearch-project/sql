/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import java.io.IOException;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.ppl.DataTypeIT;

public class NonFallbackCalciteDataTypeIT extends DataTypeIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }

  @Override
  @Test
  @Ignore("ignore this class since IP type is unsupported in calcite engine")
  public void test_nonnumeric_data_types() throws IOException {
    super.test_nonnumeric_data_types();
  }
}
