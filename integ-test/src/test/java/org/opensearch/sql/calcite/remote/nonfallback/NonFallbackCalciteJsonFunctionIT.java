/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import org.junit.Ignore;
import org.opensearch.sql.calcite.remote.fallback.CalciteJsonFunctionsIT;

@Ignore("https://github.com/opensearch-project/sql/issues/3436")
public class NonFallbackCalciteJsonFunctionIT extends CalciteJsonFunctionsIT {
  @Override
  public void init() throws Exception {
    super.init();
    disallowCalciteFallback();
  }
}
