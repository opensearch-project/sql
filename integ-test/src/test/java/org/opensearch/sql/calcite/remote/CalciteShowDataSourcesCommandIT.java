/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.junit.Ignore;
import org.opensearch.sql.ppl.ShowDataSourcesCommandIT;

@Ignore("https://github.com/opensearch-project/sql/issues/3465")
public class CalciteShowDataSourcesCommandIT extends ShowDataSourcesCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }
}
