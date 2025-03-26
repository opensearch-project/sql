/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import java.io.IOException;
import org.junit.Ignore;
import org.opensearch.sql.calcite.remote.fallback.CalciteWhereCommandIT;

public class NonFallbackCalciteWhereCommandIT extends CalciteWhereCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    disallowCalciteFallback();
  }

  @Ignore("https://github.com/opensearch-project/sql/issues/3428")
  @Override
  public void testIsNotNullFunction() throws IOException {}

  @Override
  protected String getIncompatibleTypeErrMsg() {
    return "In expression types are incompatible: fields type LONG, values type [INTEGER, INTEGER,"
        + " STRING]";
  }
}
