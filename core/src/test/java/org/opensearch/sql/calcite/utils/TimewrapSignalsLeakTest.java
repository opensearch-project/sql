/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.executor.analytics.TimewrapSignals;

/**
 * Leak guard for the timewrap pivot signals. {@code visitTimewrap} stashes its pivot state in the
 * static {@link CalcitePlanContext} thread-locals read by {@link TimewrapPivot#isTimewrap()}. These
 * are set on a pooled worker thread, so if they are not cleared at the end of the query lifecycle
 * they would leak onto the next, non-timewrap query that reuses the same thread and wrongly pivot
 * its result. This test asserts the lifecycle guard ({@code CalcitePlanContext.run}'s finally)
 * clears them.
 */
public class TimewrapSignalsLeakTest {

  @AfterEach
  public void cleanUp() {
    CalcitePlanContext.clearTimewrapSignals();
  }

  @Test
  public void timewrapSignalsDoNotLeakOntoNextQueryOnSameThread() {
    Settings settings = mock(Settings.class);
    lenient().when(settings.getSettingValue(Key.PPL_SYNTAX_LEGACY_PREFERRED)).thenReturn(false);

    // First "query" behaves like a timewrap query: it sets the pivot signals mid-flight, exactly as
    // visitTimewrap does. run()'s finally must clear them before returning.
    CalcitePlanContext.run(
        () -> {
          CalcitePlanContext.stripNullColumns.set(true);
          CalcitePlanContext.timewrapUnitName.set("1|day|days|_before");
          CalcitePlanContext.timewrapSeries.set("relative");
        },
        settings);

    // Same thread, next query: the pivot gate must be closed.
    assertFalse(
        TimewrapPivot.isTimewrap(),
        "timewrap signals leaked onto the next query on the same pooled thread");

    // And a plain (non-timewrap) result must pass through the pivot untouched — no __base_offset__
    // or __period__ artifact columns, same row.
    List<Column> columns =
        List.of(
            new Column("host", null, ExprCoreType.STRING),
            new Column("count", null, ExprCoreType.LONG));
    List<ExprValue> rows =
        List.of(ExprValueUtils.tupleValue(ImmutableMap.of("host", "h1", "count", 42L)));

    TimewrapPivot.Result result =
        TimewrapPivot.pivot(
            columns,
            rows,
            CalcitePlanContext.timewrapUnitName.get(),
            CalcitePlanContext.timewrapSeries.get());

    assertEquals(
        List.of("host", "count"),
        result.columns().stream().map(Column::getName).toList(),
        "non-timewrap result gained artifact columns after a prior timewrap query");
    assertEquals(rows, result.values());
  }

  @Test
  public void timewrapSignalsCanMoveAcrossInitPlanExecution() {
    CalcitePlanContext.stripNullColumns.set(true);
    CalcitePlanContext.timewrapUnitName.set("1|day|days|_before");
    CalcitePlanContext.timewrapSeries.set("relative");

    TimewrapSignals snapshot = TimewrapSignals.captureAndClear();
    assertFalse(TimewrapPivot.isTimewrap());

    snapshot.install();
    assertTrue(TimewrapPivot.isTimewrap());
    assertEquals("1|day|days|_before", CalcitePlanContext.timewrapUnitName.get());
    assertEquals("relative", CalcitePlanContext.timewrapSeries.get());
  }
}
