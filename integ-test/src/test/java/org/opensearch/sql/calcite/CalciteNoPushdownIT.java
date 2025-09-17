/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.opensearch.sql.calcite.remote.*;
import org.opensearch.sql.calcite.tpch.CalcitePPLTpchIT;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * This test suite runs all remote Calcite integration tests without pushdown enabled.
 *
 * <p>Individual tests in this suite will be executed independently with pushdown enabled.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  CalciteExplainIT.class
})
public class CalciteNoPushdownIT {
  private static boolean wasPushdownEnabled;

  @BeforeClass
  public static void disablePushdown() {
    wasPushdownEnabled = PPLIntegTestCase.GlobalPushdownConfig.enabled;
    PPLIntegTestCase.GlobalPushdownConfig.enabled = false;
  }

  @AfterClass
  public static void restorePushdown() {
    PPLIntegTestCase.GlobalPushdownConfig.enabled = wasPushdownEnabled;
  }
}
