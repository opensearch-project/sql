/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.junit.Ignore;
import org.opensearch.sql.ppl.ExplainIT;

public class CalciteExplainIT extends ExplainIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }

  @Override
  public void testTrendlinePushDownExplain() throws Exception {
    withFallbackEnabled(
        () -> {
          try {
            super.testTrendlinePushDownExplain();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        "https://github.com/opensearch-project/sql/issues/3466");
  }

  @Override
  public void testTrendlineWithSortPushDownExplain() throws Exception {
    withFallbackEnabled(
        () -> {
          try {
            super.testTrendlineWithSortPushDownExplain();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        "https://github.com/opensearch-project/sql/issues/3466");
  }

  @Override
  @Ignore("test only in v2")
  public void testExplainModeUnsupportedInV2() throws IOException {}
}
