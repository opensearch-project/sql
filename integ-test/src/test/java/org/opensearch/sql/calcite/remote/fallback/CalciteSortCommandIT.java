/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.fallback;

import java.io.IOException;
import org.junit.Ignore;
import org.opensearch.sql.ppl.SortCommandIT;

public class CalciteSortCommandIT extends SortCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  // TODO: Unsupported conversion for OpenSearch Data type: IP, addressed by issue:
  // https://github.com/opensearch-project/sql/issues/3395
  @Ignore
  @Override
  public void testSortIpField() throws IOException {}
}
