/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.ast.tree.Trendline.TrendlineType.SMA;
import static org.opensearch.sql.ast.tree.Trendline.TrendlineType.WMA;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.SemanticCheckException;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
public class TrendlineOperatorTest extends PhysicalPlanTestBase {
  @Mock private PhysicalPlan inputPlan;

  static Stream<Arguments> supportedDataTypes() {
    return Stream.of(SMA, WMA)
        .flatMap(
            trendlineType ->
                Stream.of(
                    Arguments.of(trendlineType, ExprCoreType.SHORT),
                    Arguments.of(trendlineType, ExprCoreType.INTEGER),
                    Arguments.of(trendlineType, ExprCoreType.LONG),
                    Arguments.of(trendlineType, ExprCoreType.FLOAT),
                    Arguments.of(trendlineType, ExprCoreType.DOUBLE)));
  }

  static Stream<Arguments> invalidArguments() {
    return Stream.of(SMA, WMA)
        .flatMap(
            trendlineType ->
                Stream.of(
                    // WMA
                    Arguments.of(
                        2,
                        AstDSL.field("distance"),
                        "distance_alias",
                        trendlineType,
                        ExprCoreType.ARRAY,
                        "DateType - Array"),
                    Arguments.of(
                        -100,
                        AstDSL.field("distance"),
                        "distance_alias",
                        trendlineType,
                        ExprCoreType.INTEGER,
                        "DataPoints - Negative"),
                    Arguments.of(
                        0,
                        AstDSL.field("distance"),
                        "distance_alias",
                        trendlineType,
                        ExprCoreType.INTEGER,
                        "DataPoints - zero")));
  }

  @Test
  public void calculates_simple_moving_average_one_field_one_sample() {
    mockPlanWithData(List.of(tupleValue(ImmutableMap.of("distance", 100, "time", 10))));
    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(1, AstDSL.field("distance"), "distance_alias", SMA),
                    ExprCoreType.DOUBLE)));

    List<ExprValue> result = execute(plan);
    assertEquals(1, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            tupleValue(ImmutableMap.of("distance", 100, "time", 10, "distance_alias", 100))));
  }

  @Test
  public void calculates_simple_moving_average_one_field_two_samples() {
    mockPlanWithData(
        List.of(
            tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 10))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", SMA),
                    ExprCoreType.DOUBLE)));

    List<ExprValue> result = execute(plan);
    assertEquals(2, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 10, "distance_alias", 150.0))));
  }

  @Test
  public void calculates_simple_moving_average_one_field_two_samples_three_rows() {
    mockPlanWithData(
        List.of(
            tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 10)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 10))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", SMA),
                    ExprCoreType.DOUBLE)));

    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 10, "distance_alias", 150.0)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 10, "distance_alias", 200.0))));
  }

  @Test
  public void calculates_simple_moving_average_multiple_computations() {
    mockPlanWithData(
        List.of(
            tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 20)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 20))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Arrays.asList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", SMA),
                    ExprCoreType.DOUBLE),
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("time"), "time_alias", SMA),
                    ExprCoreType.DOUBLE)));

    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            tupleValue(
                ImmutableMap.of(
                    "distance", 200, "time", 20, "distance_alias", 150.0, "time_alias", 15.0)),
            tupleValue(
                ImmutableMap.of(
                    "distance", 200, "time", 20, "distance_alias", 200.0, "time_alias", 20.0))));
  }

  @Test
  public void alias_overwrites_input_field() {
    mockPlanWithData(
        List.of(
            tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 10)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 10))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("distance"), "time", SMA),
                    ExprCoreType.DOUBLE)));

    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            tupleValue(ImmutableMap.of("distance", 100)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 150.0)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 200.0))));
  }

  @Test
  public void calculates_simple_moving_average_one_field_two_samples_three_rows_null_value() {
    mockPlanWithData(
        List.of(
            tupleValue(ImmutableMap.of("time", 10)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 10)),
            tupleValue(ImmutableMap.of("distance", 300, "time", 10))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", SMA),
                    ExprCoreType.DOUBLE)));

    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            tupleValue(ImmutableMap.of("time", 10)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 10)),
            tupleValue(ImmutableMap.of("distance", 300, "time", 10, "distance_alias", 250.0))));
  }

  @Test
  public void use_null_value() {
    mockPlanWithData(
        List.of(
            tupleValue(ImmutableMap.of("time", 10)),
            tupleValue(ImmutableMap.of("distance", ExprNullValue.of(), "time", 10)),
            tupleValue(ImmutableMap.of("distance", 100, "time", 10))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(1, AstDSL.field("distance"), "distance_alias", SMA),
                    ExprCoreType.DOUBLE)));

    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            tupleValue(ImmutableMap.of("time", 10)),
            tupleValue(ImmutableMap.of("distance", ExprNullValue.of(), "time", 10)),
            tupleValue(ImmutableMap.of("distance", 100, "time", 10, "distance_alias", 100))));
  }

  @Test
  public void calculates_simple_moving_average_date() {
    mockPlanWithData(
        List.of(
            tupleValue(ImmutableMap.of("date", ExprValueUtils.dateValue(LocalDate.EPOCH))),
            tupleValue(
                ImmutableMap.of("date", ExprValueUtils.dateValue(LocalDate.EPOCH.plusDays(6)))),
            tupleValue(
                ImmutableMap.of("date", ExprValueUtils.dateValue(LocalDate.EPOCH.plusDays(12))))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("date"), "date_alias", SMA),
                    ExprCoreType.DATE)));

    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            tupleValue(ImmutableMap.of("date", ExprValueUtils.dateValue(LocalDate.EPOCH))),
            tupleValue(
                ImmutableMap.of(
                    "date",
                    ExprValueUtils.dateValue(LocalDate.EPOCH.plusDays(6)),
                    "date_alias",
                    ExprValueUtils.dateValue(LocalDate.EPOCH.plusDays(3)))),
            tupleValue(
                ImmutableMap.of(
                    "date",
                    ExprValueUtils.dateValue(LocalDate.EPOCH.plusDays(12)),
                    "date_alias",
                    ExprValueUtils.dateValue(LocalDate.EPOCH.plusDays(9))))));
  }

  @Test
  public void calculates_simple_moving_average_time() {
    mockPlanWithData(
        List.of(
            tupleValue(ImmutableMap.of("time", ExprValueUtils.timeValue(LocalTime.MIN))),
            tupleValue(
                ImmutableMap.of("time", ExprValueUtils.timeValue(LocalTime.MIN.plusHours(6)))),
            tupleValue(
                ImmutableMap.of("time", ExprValueUtils.timeValue(LocalTime.MIN.plusHours(12))))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("time"), "time_alias", SMA),
                    ExprCoreType.TIME)));

    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            tupleValue(ImmutableMap.of("time", LocalTime.MIN)),
            tupleValue(
                ImmutableMap.of(
                    "time", LocalTime.MIN.plusHours(6), "time_alias", LocalTime.MIN.plusHours(3))),
            tupleValue(
                ImmutableMap.of(
                    "time",
                    LocalTime.MIN.plusHours(12),
                    "time_alias",
                    LocalTime.MIN.plusHours(9)))));
  }

  @Test
  public void calculates_simple_moving_average_timestamp() {
    mockPlanWithData(
        List.of(
            tupleValue(ImmutableMap.of("timestamp", ExprValueUtils.timestampValue(Instant.EPOCH))),
            tupleValue(
                ImmutableMap.of(
                    "timestamp", ExprValueUtils.timestampValue(Instant.EPOCH.plusMillis(1000)))),
            tupleValue(
                ImmutableMap.of(
                    "timestamp", ExprValueUtils.timestampValue(Instant.EPOCH.plusMillis(1500))))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("timestamp"), "timestamp_alias", SMA),
                    ExprCoreType.TIMESTAMP)));

    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            tupleValue(ImmutableMap.of("timestamp", Instant.EPOCH)),
            tupleValue(
                ImmutableMap.of(
                    "timestamp",
                    Instant.EPOCH.plusMillis(1000),
                    "timestamp_alias",
                    Instant.EPOCH.plusMillis(500))),
            tupleValue(
                ImmutableMap.of(
                    "timestamp",
                    Instant.EPOCH.plusMillis(1500),
                    "timestamp_alias",
                    Instant.EPOCH.plusMillis(1250)))));
  }

  @Test
  public void calculates_weighted_moving_average_one_field_one_sample() {
    mockPlanWithData(List.of(tupleValue(ImmutableMap.of("distance", 100, "time", 10))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(1, AstDSL.field("distance"), "distance_alias", WMA),
                    ExprCoreType.DOUBLE)));

    plan.open();

    assertTrue(plan.hasNext());
    assertEquals(
        tupleValue(ImmutableMap.of("distance", 100, "time", 10, "distance_alias", 100)),
        plan.next());
  }

  @Test
  public void calculates_weighted_moving_average_one_field_two_samples() {
    mockPlanWithData(
        List.of(
            tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 10))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", WMA),
                    ExprCoreType.DOUBLE)));

    List<ExprValue> result = execute(plan);
    assertEquals(2, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            tupleValue(
                ImmutableMap.of(
                    "distance", 200, "time", 10, "distance_alias", 166.66666666666663))));
  }

  @Test
  public void calculates_weighted_moving_average_one_field_two_samples_three_rows() {
    mockPlanWithData(
        List.of(
            tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 10)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 10))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", WMA),
                    ExprCoreType.DOUBLE)));

    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            tupleValue(
                ImmutableMap.of("distance", 200, "time", 10, "distance_alias", 166.66666666666663)),
            tupleValue(
                ImmutableMap.of(
                    "distance", 200, "time", 10, "distance_alias", 199.99999999999997))));
  }

  @Test
  public void calculates_weighted_moving_average_multiple_computations() {
    mockPlanWithData(
        List.of(
            tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 20)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 20))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Arrays.asList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", WMA),
                    ExprCoreType.DOUBLE),
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("time"), "time_alias", WMA),
                    ExprCoreType.DOUBLE)));

    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            tupleValue(
                ImmutableMap.of(
                    "distance",
                    200,
                    "time",
                    20,
                    "distance_alias",
                    166.66666666666663,
                    "time_alias",
                    16.666666666666664)),
            tupleValue(
                ImmutableMap.of(
                    "distance",
                    200,
                    "time",
                    20,
                    "distance_alias",
                    199.99999999999997,
                    "time_alias",
                    20.0))));
  }

  @Test
  public void calculates_weighted_moving_average_one_field_two_samples_three_rows_null_value() {
    mockPlanWithData(
        List.of(
            tupleValue(ImmutableMap.of("time", 10)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 10)),
            tupleValue(ImmutableMap.of("distance", 300, "time", 10))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", WMA),
                    ExprCoreType.DOUBLE)));

    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            tupleValue(ImmutableMap.of("time", 10)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 10)),
            tupleValue(
                ImmutableMap.of(
                    "distance", 300, "time", 10, "distance_alias", 266.66666666666663))));
  }

  @Test
  public void calculates_weighted_moving_average_date() {
    mockPlanWithData(
        List.of(
            tupleValue(ImmutableMap.of("date", ExprValueUtils.dateValue(LocalDate.EPOCH))),
            tupleValue(
                ImmutableMap.of("date", ExprValueUtils.dateValue(LocalDate.EPOCH.plusDays(6)))),
            tupleValue(
                ImmutableMap.of("date", ExprValueUtils.dateValue(LocalDate.EPOCH.plusDays(12))))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("date"), "date_alias", WMA),
                    ExprCoreType.DATE)));

    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            tupleValue(ImmutableMap.of("date", ExprValueUtils.dateValue(LocalDate.EPOCH))),
            tupleValue(
                ImmutableMap.of(
                    "date",
                    ExprValueUtils.dateValue(LocalDate.EPOCH.plusDays(6)),
                    "date_alias",
                    ExprValueUtils.dateValue(LocalDate.EPOCH.plusDays(4)))),
            tupleValue(
                ImmutableMap.of(
                    "date",
                    ExprValueUtils.dateValue(LocalDate.EPOCH.plusDays(12)),
                    "date_alias",
                    ExprValueUtils.dateValue(LocalDate.EPOCH.plusDays(10))))));
  }

  @Test
  public void calculates_weighted_moving_average_time() {
    mockPlanWithData(
        List.of(
            tupleValue(ImmutableMap.of("time", ExprValueUtils.timeValue(LocalTime.MIN))),
            tupleValue(
                ImmutableMap.of("time", ExprValueUtils.timeValue(LocalTime.MIN.plusHours(6)))),
            tupleValue(
                ImmutableMap.of("time", ExprValueUtils.timeValue(LocalTime.MIN.plusHours(12))))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("time"), "time_alias", WMA),
                    ExprCoreType.TIME)));

    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            tupleValue(ImmutableMap.of("time", LocalTime.MIN)),
            tupleValue(
                ImmutableMap.of(
                    "time", LocalTime.MIN.plusHours(6), "time_alias", LocalTime.MIN.plusHours(4))),
            tupleValue(
                ImmutableMap.of(
                    "time",
                    LocalTime.MIN.plusHours(12),
                    "time_alias",
                    LocalTime.MIN.plusHours(10)))));
  }

  @Test
  public void calculates_weighted_moving_average_timestamp() {
    mockPlanWithData(
        List.of(
            tupleValue(ImmutableMap.of("timestamp", ExprValueUtils.timestampValue(Instant.EPOCH))),
            tupleValue(
                ImmutableMap.of(
                    "timestamp", ExprValueUtils.timestampValue(Instant.EPOCH.plusMillis(1000)))),
            tupleValue(
                ImmutableMap.of(
                    "timestamp", ExprValueUtils.timestampValue(Instant.EPOCH.plusMillis(1500))))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("timestamp"), "timestamp_alias", WMA),
                    ExprCoreType.TIMESTAMP)));

    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            tupleValue(ImmutableMap.of("timestamp", Instant.EPOCH)),
            tupleValue(
                ImmutableMap.of(
                    "timestamp",
                    Instant.EPOCH.plusMillis(1000),
                    "timestamp_alias",
                    Instant.EPOCH.plusMillis(667))),
            tupleValue(
                ImmutableMap.of(
                    "timestamp",
                    Instant.EPOCH.plusMillis(1500),
                    "timestamp_alias",
                    Instant.EPOCH.plusMillis(1333)))));
  }

  @ParameterizedTest
  @MethodSource("supportedDataTypes")
  public void trendLine_dataType_support(
      Trendline.TrendlineType trendlineType, ExprCoreType supportedType) {
    mockPlanWithData(
        List.of(
            tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 10)),
            tupleValue(ImmutableMap.of("distance", 200, "time", 10))));

    var plan =
        new TrendlineOperator(
            inputPlan,
            Collections.singletonList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", WMA),
                    supportedType)));

    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(
        String.format(
            "Assertion error on TrendLine-WMA dataType support: %s", supportedType.typeName()),
        result,
        containsInAnyOrder(
            tupleValue(ImmutableMap.of("distance", 100, "time", 10)),
            tupleValue(
                ImmutableMap.of("distance", 200, "time", 10, "distance_alias", 166.66666666666663)),
            tupleValue(
                ImmutableMap.of(
                    "distance", 200, "time", 10, "distance_alias", 199.99999999999997))));
  }

  @ParameterizedTest
  @MethodSource("invalidArguments")
  public void use_invalid_configuration(
      Integer dataPoints,
      Field field,
      String alias,
      Trendline.TrendlineType trendlineType,
      ExprCoreType dataType,
      String errorMessage) {
    assertThrows(
        SemanticCheckException.class,
        () ->
            new TrendlineOperator(
                inputPlan,
                Collections.singletonList(
                    Pair.of(
                        AstDSL.computation(dataPoints, field, alias, trendlineType), dataType))),
        "Unsupported arguments: " + errorMessage);
  }

  private void mockPlanWithData(List<ExprValue> inputs) {
    List<Boolean> hasNextElements = new ArrayList<>(Collections.nCopies(inputs.size(), true));
    hasNextElements.add(false);

    Iterator<Boolean> hasNextIterator = hasNextElements.iterator();
    when(inputPlan.hasNext())
        .thenAnswer(i -> hasNextIterator.hasNext() ? hasNextIterator.next() : null);
    Iterator<ExprValue> iterator = inputs.iterator();
    when(inputPlan.next()).thenAnswer(i -> iterator.hasNext() ? iterator.next() : null);
  }
}
