/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.Capability.DEDUP_NONDETERMINISTIC;

import java.io.IOException;
import org.opensearch.sql.ppl.DedupCommandIT;
import org.opensearch.sql.util.RequiresCapability;

public class CalciteDedupCommandIT extends DedupCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @RequiresCapability(
      value = DEDUP_NONDETERMINISTIC,
      note =
          "consecutive dedup falls back to V2 on the Calcite path, but the AE route has no working"
              + " V2 fallback (DEDUP_NONDETERMINISTIC).")
  @Override
  public void testConsecutiveDedup() throws IOException {
    withFallbackEnabled(
        () -> {
          try {
            super.testConsecutiveDedup();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        "https://github.com/opensearch-project/sql/issues/3415");
  }
}
