/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.ScanAggregates.MultiAggregateInfo;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.setting.Settings.Key;

/**
 * The per-query scan-aggregate info feeding the partial-result gate. It must not outlive its query.
 */
public class PartialResultGateTest {

  @BeforeEach
  public void reset() {
    CalcitePlanContext.clearTimewrapSignals();
  }

  @AfterEach
  public void cleanUp() {
    CalcitePlanContext.clearTimewrapSignals();
  }

  @Test
  public void infoFollowsWhatTheLastCompileFound() {
    assertTrue(CalcitePlanContext.getMultiAggregateInfo().unionFieldsByTable().isEmpty());

    CalcitePlanContext.setMultiAggregateInfo(
        new MultiAggregateInfo(Map.of("OpenSearch.idx", List.of("env")), Set.of(), false));
    assertTrue(
        CalcitePlanContext.getMultiAggregateInfo()
            .unionFieldsFor("OpenSearch.idx")
            .contains("env"));

    // Derived per compile, so the next plan replaces it.
    CalcitePlanContext.setMultiAggregateInfo(MultiAggregateInfo.EMPTY);
    assertFalse(CalcitePlanContext.getMultiAggregateInfo().barred("OpenSearch.idx"));
  }

  @Test
  public void infoDoesNotLeakOntoNextQueryOnSameThread() {
    Settings settings = mock(Settings.class);
    lenient().when(settings.getSettingValue(Key.PPL_SYNTAX_LEGACY_PREFERRED)).thenReturn(false);

    // First query marks mid-flight; run()'s finally must clear it.
    CalcitePlanContext.run(
        () ->
            CalcitePlanContext.setMultiAggregateInfo(
                new MultiAggregateInfo(Map.of(), Set.of("OpenSearch.idx"), false)),
        settings);

    assertFalse(
        CalcitePlanContext.getMultiAggregateInfo().barred("OpenSearch.idx"),
        "the scan-aggregate info leaked onto the next query on the same pooled thread");
  }
}
