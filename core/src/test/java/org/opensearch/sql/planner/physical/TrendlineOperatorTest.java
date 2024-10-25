/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.data.model.ExprValueUtils;

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
                AstDSL.computation(1, AstDSL.field("distance"), "distance_alias", "sma")));

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
                AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", "sma")));

    plan.open();
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(ImmutableMap.of("distance", 100, "time", 10)), plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        plan.next(),
        ExprValueUtils.tupleValue(
            ImmutableMap.of("distance", 200, "time", 10, "distance_alias", 150.0)));
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
                AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", "sma")));

    plan.open();
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(ImmutableMap.of("distance", 100, "time", 10)), plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        plan.next(),
        ExprValueUtils.tupleValue(
            ImmutableMap.of("distance", 200, "time", 10, "distance_alias", 150.0)));
    assertTrue(plan.hasNext());
    assertEquals(
        plan.next(),
        ExprValueUtils.tupleValue(
            ImmutableMap.of("distance", 200, "time", 10, "distance_alias", 200.0)));
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
                AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", "sma"),
                AstDSL.computation(2, AstDSL.field("time"), "time_alias", "sma")));

    plan.open();
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(ImmutableMap.of("distance", 100, "time", 10)), plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        plan.next(),
        ExprValueUtils.tupleValue(
            ImmutableMap.of(
                "distance", 200, "time", 20, "distance_alias", 150.0, "time_alias", 15.0)));
    assertTrue(plan.hasNext());
    assertEquals(
        plan.next(),
        ExprValueUtils.tupleValue(
            ImmutableMap.of(
                "distance", 200, "time", 20, "distance_alias", 200.0, "time_alias", 20.0)));
    assertFalse(plan.hasNext());
  }

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
                AstDSL.computation(2, AstDSL.field("distance"), "time", "sma")));

    plan.open();
    assertTrue(plan.hasNext());
    assertEquals(
        ExprValueUtils.tupleValue(ImmutableMap.of("distance", 100, "time", 100)), plan.next());
    assertTrue(plan.hasNext());
    assertEquals(
        plan.next(), ExprValueUtils.tupleValue(ImmutableMap.of("distance", 200, "time", 150.0)));
    assertTrue(plan.hasNext());
    assertEquals(
        plan.next(), ExprValueUtils.tupleValue(ImmutableMap.of("distance", 200, "time", 200.0)));
    assertFalse(plan.hasNext());
  }
}
