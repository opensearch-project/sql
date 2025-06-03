/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import org.junit.Ignore;
import org.opensearch.sql.calcite.remote.fallback.CalciteInformationSchemaCommandIT;

@Ignore("https://github.com/opensearch-project/sql/issues/3455")
public class NonFallbackCalciteInformationSchemaCommandIT
    extends CalciteInformationSchemaCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    disallowCalciteFallback();
  }
}
