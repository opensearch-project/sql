/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import org.junit.Ignore;
import org.opensearch.sql.calcite.remote.fallback.CalciteObjectFieldOperateIT;

@Ignore("https://github.com/opensearch-project/sql/issues/3452")
public class NonFallbackCalciteObjectFieldOperateIT extends CalciteObjectFieldOperateIT {
  @Override
  public void init() throws Exception {
    super.init();
    disallowCalciteFallback();
  }
}
