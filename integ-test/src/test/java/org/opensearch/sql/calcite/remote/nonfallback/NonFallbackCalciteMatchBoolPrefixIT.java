/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import org.junit.Ignore;
import org.opensearch.sql.ppl.MatchBoolPrefixIT;

@Ignore("Search Functions are not supported")
public class NonFallbackCalciteMatchBoolPrefixIT extends MatchBoolPrefixIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }
}
