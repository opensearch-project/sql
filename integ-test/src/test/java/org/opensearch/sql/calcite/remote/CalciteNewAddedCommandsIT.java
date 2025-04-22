/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.opensearch.sql.ppl.NewAddedCommandsIT;

public class CalciteNewAddedCommandsIT extends NewAddedCommandsIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }

  @Override
  public void testJoin() {
    // fallback disabled, should pass
    super.testJoin();
    // fallback enabled, should pass too
    withFallbackEnabled(super::testJoin, "");
  }

  @Override
  public void testLookup() {
    // fallback disabled, should pass
    super.testLookup();
    // fallback enabled, should pass too
    withFallbackEnabled(super::testLookup, "");
  }

  @Override
  public void testSubsearch() {
    // fallback disabled, should pass
    super.testSubsearch();
    // fallback enabled, should pass too
    withFallbackEnabled(super::testSubsearch, "");
  }
}
