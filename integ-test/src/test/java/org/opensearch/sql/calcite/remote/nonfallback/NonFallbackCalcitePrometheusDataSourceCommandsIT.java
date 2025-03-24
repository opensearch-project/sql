/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import org.junit.Ignore;
import org.opensearch.sql.calcite.remote.fallback.CalcitePrometheusDataSourceCommandsIT;

@Ignore("https://github.com/opensearch-project/sql/issues/3455")
public class NonFallbackCalcitePrometheusDataSourceCommandsIT
    extends CalcitePrometheusDataSourceCommandsIT {
  @Override
  public void init() throws Exception {
    super.init();
    disallowCalciteFallback();
  }
}
