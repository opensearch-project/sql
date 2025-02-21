/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.ppl.DataTypeIT;

public class CalciteDataTypeIT extends DataTypeIT {

  @Override
  public void init() throws IOException {
    enableCalcite();
    disallowCalciteFallback();
    super.init();
  }

  @Override
  @Test
  @Ignore("ignore this class since IP type is unsupported in calcite engine")
  public void test_nonnumeric_data_types() throws IOException {
    super.test_nonnumeric_data_types();
  }

  @Override
  @Test
  @Ignore("ignore this class since the data type is unmatched in calcite engine")
  public void test_numeric_data_types() throws IOException {
    super.test_numeric_data_types();
  }
}
