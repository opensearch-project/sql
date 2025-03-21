/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import org.junit.Ignore;
import org.opensearch.sql.ppl.TrendlineCommandIT;

@Ignore("https://github.com/opensearch-project/sql/issues/3466")
public class NonFallbackCalciteTrendlineCommandIT extends TrendlineCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }
}
