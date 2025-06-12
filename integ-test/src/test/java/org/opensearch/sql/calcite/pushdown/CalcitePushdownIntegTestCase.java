/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.calcite.pushdown;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.opensearch.sql.calcite.remote.CalcitePPLBasicIT;
import org.opensearch.sql.calcite.remote.CalcitePPLCaseFunctionIT;
import org.opensearch.sql.ppl.PPLIntegTestCase;

@RunWith(Suite.class)
@Suite.SuiteClasses({CalcitePPLBasicIT.class, CalcitePPLCaseFunctionIT.class})
public class CalcitePushdownIntegTestCase {
  @BeforeClass
  public static void disablePushdown() {
    PPLIntegTestCase.GlobalPushdownConfig.enabled = false;
  }

  @AfterClass
  public static void enablePushdown() {
    PPLIntegTestCase.GlobalPushdownConfig.enabled = true;
  }
}
