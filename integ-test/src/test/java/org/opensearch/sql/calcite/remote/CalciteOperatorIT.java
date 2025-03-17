/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.junit.Ignore;
import org.opensearch.sql.ppl.OperatorIT;

public class CalciteOperatorIT extends OperatorIT {

  @Override
  public void init() throws IOException {
    enableCalcite();
    disallowCalciteFallback();
    super.init();
  }

  @Ignore("https://github.com/opensearch-project/sql/issues/3398")
  @Override
  public void testModuleOperator() throws IOException {
    super.testModuleOperator();
  }
}
