/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import org.junit.Ignore;
import org.opensearch.sql.calcite.remote.fallback.CalciteLikeQueryIT;

// TODO Like function behaviour in V2 is not correct. Remove when it was fixed in V2.
@Ignore("https://github.com/opensearch-project/sql/issues/3428")
public class NonFallbackCalciteLikeQueryIT extends CalciteLikeQueryIT {
  @Override
  public void init() throws Exception {
    super.init();
    disallowCalciteFallback();
  }
}
