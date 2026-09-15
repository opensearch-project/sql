/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.setting.Settings.Key;

/**
 * The gate that bars partial-result mode for {@code chart} and {@code timechart}. It lives in a
 * static thread-local set while building the plan and read during pushdown, so it must not survive
 * the query that set it: the next query on the same pooled worker thread would silently lose
 * partial mode.
 */
public class ChartPartialResultGateTest {

  @AfterEach
  public void cleanUp() {
    CalcitePlanContext.clearTimewrapSignals();
  }

  @Test
  public void gateIsClosedUntilAChartIsPlanned() {
    assertFalse(CalcitePlanContext.isChartPlanned());

    CalcitePlanContext.markChartPlanned();

    assertTrue(CalcitePlanContext.isChartPlanned());
  }

  @Test
  public void gateDoesNotLeakOntoNextQueryOnSameThread() {
    Settings settings = mock(Settings.class);
    lenient().when(settings.getSettingValue(Key.PPL_SYNTAX_LEGACY_PREFERRED)).thenReturn(false);

    // First query charts, as visitChart does mid-flight. run()'s finally must clear the mark.
    CalcitePlanContext.run(CalcitePlanContext::markChartPlanned, settings);

    assertFalse(
        CalcitePlanContext.isChartPlanned(),
        "the chart gate leaked onto the next query on the same pooled thread");
  }
}
