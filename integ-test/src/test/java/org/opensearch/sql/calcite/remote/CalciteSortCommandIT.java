/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.junit.Ignore;
import org.opensearch.sql.ppl.SortCommandIT;

public class CalciteSortCommandIT extends SortCommandIT {
  @Override
  public void init() throws IOException {
    enableCalcite();
    disallowCalciteFallback();
    super.init();
  }

  // TODO: Unsupported conversion for OpenSearch Data type: IP, addressed by issue:
  // https://github.com/opensearch-project/sql/issues/3322
  @Ignore
  @Override
  public void testSortIpField() throws IOException {}

  // TODO: Fix incorrect results for NULL values, addressed by issue:
  // https://github.com/opensearch-project/sql/issues/3375
  @Ignore
  @Override
  public void testSortWithNullValue() throws IOException {}
}
