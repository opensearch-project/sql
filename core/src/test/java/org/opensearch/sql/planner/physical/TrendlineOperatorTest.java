/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
public class TrendlineOperatorTest {
  @Mock private PhysicalPlan inputPlan;

  @Test
  public void calculates_simple_moving_average_one_field_one_sample() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("distance", 100, "time", 10)));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(1, AstDSL.field("distance"), "distance_alias", "sma"),
                    ExprCoreType.DOUBLE)));

    plan.open();
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(
            ImmutableMap.of("distance", 100, "time", 10, "distance_alias", 100)),
        plan.next());
  }

  @Test
  public void calculates_simple_moving_average_one_field_two_samples() {
    when(inputPlan.hasNext()).thenReturn(true, true, false);
    when(inputPlan.next())
        .thenReturn(
            ExprValueUtils.tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            ExprValueUtils.tupleValue(ImmutableMap.of("distance", 200, "time", 10)));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", "sma"),
                    ExprCoreType.DOUBLE)));

    plan.open();
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(ImmutableMap.of("distance", 100, "time", 10)), plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(
            ImmutableMap.of("distance", 200, "time", 10, "distance_alias", 150.0)),
        plan.next());
    assertFalse(plan.hasNext());
  }

  @Test
  public void calculates_simple_moving_average_one_field_two_samples_three_rows() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, false);
    when(inputPlan.next())
        .thenReturn(
            ExprValueUtils.tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            ExprValueUtils.tupleValue(ImmutableMap.of("distance", 200, "time", 10)),
            ExprValueUtils.tupleValue(ImmutableMap.of("distance", 200, "time", 10)));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", "sma"),
                    ExprCoreType.DOUBLE)));

    plan.open();
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(ImmutableMap.of("distance", 100, "time", 10)), plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(
            ImmutableMap.of("distance", 200, "time", 10, "distance_alias", 150.0)),
        plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(
            ImmutableMap.of("distance", 200, "time", 10, "distance_alias", 200.0)),
        plan.next());
    assertFalse(plan.hasNext());
  }

  @Test
  public void calculates_simple_moving_average_multiple_computations() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, false);
    when(inputPlan.next())
        .thenReturn(
            ExprValueUtils.tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            ExprValueUtils.tupleValue(ImmutableMap.of("distance", 200, "time", 20)),
            ExprValueUtils.tupleValue(ImmutableMap.of("distance", 200, "time", 20)));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Arrays.asList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", "sma"),
                    ExprCoreType.DOUBLE),
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("time"), "time_alias", "sma"),
                    ExprCoreType.DOUBLE)));

    plan.open();
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(ImmutableMap.of("distance", 100, "time", 10)), plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(
            ImmutableMap.of(
                "distance", 200, "time", 20, "distance_alias", 150.0, "time_alias", 15.0)),
        plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(
            ImmutableMap.of(
                "distance", 200, "time", 20, "distance_alias", 200.0, "time_alias", 20.0)),
        plan.next());
    assertFalse(plan.hasNext());
  }

  @Test
  public void alias_overwrites_input_field() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, false);
    when(inputPlan.next())
        .thenReturn(
            ExprValueUtils.tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            ExprValueUtils.tupleValue(ImmutableMap.of("distance", 200, "time", 10)),
            ExprValueUtils.tupleValue(ImmutableMap.of("distance", 200, "time", 10)));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("distance"), "time", "sma"),
                    ExprCoreType.DOUBLE)));

    plan.open();
    assertTrue(plan.hasNext());
    assertEquals(ExprValueUtils.tupleValue(ImmutableMap.of("distance", 100)), plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(ImmutableMap.of("distance", 200, "time", 150.0)), plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(ImmutableMap.of("distance", 200, "time", 200.0)), plan.next());
    assertFalse(plan.hasNext());
  }

  @Test
  public void calculates_simple_moving_average_one_field_two_samples_three_rows_null_value() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, false);
    when(inputPlan.next())
        .thenReturn(
            ExprValueUtils.tupleValue(ImmutableMap.of("time", 10)),
            ExprValueUtils.tupleValue(ImmutableMap.of("distance", 200, "time", 10)),
            ExprValueUtils.tupleValue(ImmutableMap.of("distance", 300, "time", 10)));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", "sma"),
                    ExprCoreType.DOUBLE)));

    plan.open();
    assertTrue(plan.hasNext());
    assertEquals(ExprValueUtils.tupleValue(ImmutableMap.of("time", 10)), plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(ImmutableMap.of("distance", 200, "time", 10)), plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(
            ImmutableMap.of("distance", 300, "time", 10, "distance_alias", 250.0)),
        plan.next());
    assertFalse(plan.hasNext());
  }

  @Test
  public void use_null_value() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, false);
    when(inputPlan.next())
        .thenReturn(
            ExprValueUtils.tupleValue(ImmutableMap.of("time", 10)),
            ExprValueUtils.tupleValue(ImmutableMap.of("distance", ExprNullValue.of(), "time", 10)),
            ExprValueUtils.tupleValue(ImmutableMap.of("distance", 100, "time", 10)));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(1, AstDSL.field("distance"), "distance_alias", "sma"),
                    ExprCoreType.DOUBLE)));

    plan.open();
    assertTrue(plan.hasNext());
    assertEquals(ExprValueUtils.tupleValue(ImmutableMap.of("time", 10)), plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(ImmutableMap.of("distance", ExprNullValue.of(), "time", 10)),
        plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(
            ImmutableMap.of("distance", 100, "time", 10, "distance_alias", 100)),
        plan.next());
    assertFalse(plan.hasNext());
  }

  @Test
  public void use_illegal_core_type() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new TrendlineOperator(
              inputPlan,
              Collections.singletonList(
                  Pair.of(
                      AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", "sma"),
                      ExprCoreType.ARRAY)));
        });
  }

  @Test
  public void use_illegal_computation_type() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new TrendlineOperator(
              inputPlan,
              Collections.singletonList(
                  Pair.of(
                      AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", "fake"),
                      ExprCoreType.DOUBLE)));
        });
  }

  @Test
  public void calculates_simple_moving_average_date() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, false);
    when(inputPlan.next())
        .thenReturn(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("date", ExprValueUtils.dateValue(LocalDate.EPOCH))),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("date", ExprValueUtils.dateValue(LocalDate.EPOCH.plusDays(6)))),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("date", ExprValueUtils.dateValue(LocalDate.EPOCH.plusDays(12)))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("date"), "date_alias", "sma"),
                    ExprCoreType.DATE)));

    plan.open();
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(
            ImmutableMap.of("date", ExprValueUtils.dateValue(LocalDate.EPOCH))),
        plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(
            ImmutableMap.of(
                "date",
                ExprValueUtils.dateValue(LocalDate.EPOCH.plusDays(6)),
                "date_alias",
                ExprValueUtils.dateValue(LocalDate.EPOCH.plusDays(3)))),
        plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(
            ImmutableMap.of(
                "date",
                ExprValueUtils.dateValue(LocalDate.EPOCH.plusDays(12)),
                "date_alias",
                ExprValueUtils.dateValue(LocalDate.EPOCH.plusDays(9)))),
        plan.next());
    assertFalse(plan.hasNext());
  }

  @Test
  public void calculates_simple_moving_average_time() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, false);
    when(inputPlan.next())
        .thenReturn(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("time", ExprValueUtils.timeValue(LocalTime.MIN))),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("time", ExprValueUtils.timeValue(LocalTime.MIN.plusHours(6)))),
            ExprValueUtils.tupleValue(
                ImmutableMap.of("time", ExprValueUtils.timeValue(LocalTime.MIN.plusHours(12)))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("time"), "time_alias", "sma"),
                    ExprCoreType.TIME)));

    plan.open();
    assertTrue(plan.hasNext());
    assertEquals(ExprValueUtils.tupleValue(ImmutableMap.of("time", LocalTime.MIN)), plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(
            ImmutableMap.of(
                "time", LocalTime.MIN.plusHours(6), "time_alias", LocalTime.MIN.plusHours(3))),
        plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(
            ImmutableMap.of(
                "time", LocalTime.MIN.plusHours(12), "time_alias", LocalTime.MIN.plusHours(9))),
        plan.next());
    assertFalse(plan.hasNext());
  }

  @Test
  public void calculates_simple_moving_average_timestamp() {
    when(inputPlan.hasNext()).thenReturn(true, true, true, false);
    when(inputPlan.next())
        .thenReturn(
            ExprValueUtils.tupleValue(
                ImmutableMap.of("timestamp", ExprValueUtils.timestampValue(Instant.EPOCH))),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "timestamp", ExprValueUtils.timestampValue(Instant.EPOCH.plusMillis(1000)))),
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "timestamp", ExprValueUtils.timestampValue(Instant.EPOCH.plusMillis(1500)))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("timestamp"), "timestamp_alias", "sma"),
                    ExprCoreType.TIMESTAMP)));

    plan.open();
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(ImmutableMap.of("timestamp", Instant.EPOCH)), plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(
            ImmutableMap.of(
                "timestamp",
                Instant.EPOCH.plusMillis(1000),
                "timestamp_alias",
                Instant.EPOCH.plusMillis(500))),
        plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(
            ImmutableMap.of(
                "timestamp",
                Instant.EPOCH.plusMillis(1500),
                "timestamp_alias",
                Instant.EPOCH.plusMillis(1250))),
        plan.next());
    assertFalse(plan.hasNext());
  }
}
