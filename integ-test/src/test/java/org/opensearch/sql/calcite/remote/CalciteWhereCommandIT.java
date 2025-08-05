/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.opensearch.sql.ppl.WhereCommandIT;

public class CalciteWhereCommandIT extends WhereCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Override
  protected String getIncompatibleTypeErrMsg() {
    return "In expression types are incompatible: fields type LONG, values type [INTEGER, INTEGER,"
        + " STRING]";
  }
}
