/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import org.junit.Ignore;
import org.opensearch.sql.calcite.remote.fallback.CalciteExplainIT;

public class NonFallbackCalciteExplainIT extends CalciteExplainIT {
  @Override
  public void init() throws Exception {
    super.init();
    disallowCalciteFallback();
  }

  @Override
  @Ignore("https://github.com/opensearch-project/sql/issues/3461")
  public void testFillNullPushDownExplain() throws Exception {
    super.testFillNullPushDownExplain();
  }

  @Override
  @Ignore("https://github.com/opensearch-project/sql/issues/3466")
  public void testTrendlinePushDownExplain() throws Exception {
    super.testTrendlinePushDownExplain();
  }

  @Override
  @Ignore("https://github.com/opensearch-project/sql/issues/3466")
  public void testTrendlineWithSortPushDownExplain() throws Exception {
    super.testTrendlineWithSortPushDownExplain();
  }
}
