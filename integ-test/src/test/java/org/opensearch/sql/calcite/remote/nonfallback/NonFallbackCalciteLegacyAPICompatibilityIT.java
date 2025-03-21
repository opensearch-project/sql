/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import org.opensearch.sql.ppl.LegacyAPICompatibilityIT;

public class NonFallbackCalciteLegacyAPICompatibilityIT extends LegacyAPICompatibilityIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }
}
