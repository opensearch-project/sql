/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import org.junit.Ignore;
import org.opensearch.sql.ppl.MatchIT;

// Search Functions are not supported
@Ignore("https://github.com/opensearch-project/sql/issues/3462")
public class NonFallbackCalciteMatchIT extends MatchIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }
}
