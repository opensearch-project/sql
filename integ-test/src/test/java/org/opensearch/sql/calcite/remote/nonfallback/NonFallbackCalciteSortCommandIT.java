/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import java.io.IOException;
import org.junit.Ignore;
import org.opensearch.sql.calcite.remote.fallback.CalciteSortCommandIT;

public class NonFallbackCalciteSortCommandIT extends CalciteSortCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    disallowCalciteFallback();
  }

  // TODO: Unsupported conversion for OpenSearch Data type: IP, addressed by issue:
  // https://github.com/opensearch-project/sql/issues/3395
  @Ignore
  @Override
  public void testSortIpField() throws IOException {}
}
