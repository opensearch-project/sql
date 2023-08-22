/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.eval;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;

@ExtendWith(MockitoExtension.class)
class EvalOperatorTest extends PhysicalPlanTestBase {
  @Mock private PhysicalPlan inputPlan;

  @Test
  public void create_new_field_that_contain_the_result_of_a_calculation() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("distance", 100, "time", 10)));

    PhysicalPlan plan =
        eval(
            inputPlan,
            ImmutablePair.of(
                DSL.ref("velocity", DOUBLE),
                DSL.divide(DSL.ref("distance", INTEGER), DSL.ref("time", INTEGER))));
    assertThat(
        execute(plan),
        allOf(
            iterableWithSize(1),
            hasItems(
                ExprValueUtils.tupleValue(
                    ImmutableMap.of("distance", 100, "time", 10, "velocity", 10)))));
  }

  @Test
  public void create_multiple_field_using_field_defined_in_input_tuple() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("distance", 100, "time", 10)));

    PhysicalPlan plan =
        eval(
            inputPlan,
            ImmutablePair.of(
                DSL.ref("velocity", DOUBLE),
                DSL.divide(DSL.ref("distance", INTEGER), DSL.ref("time", INTEGER))),
            ImmutablePair.of(
                DSL.ref("doubleDistance", INTEGER),
                DSL.multiply(DSL.ref("distance", INTEGER), DSL.literal(2))));
    assertThat(
        execute(plan),
        allOf(
            iterableWithSize(1),
            hasItems(
                ExprValueUtils.tupleValue(
                    ImmutableMap.of(
                        "distance", 100, "time", 10, "velocity", 10, "doubleDistance", 200)))));
  }

  @Test
  public void create_multiple_filed_using_field_defined_in_eval_operator() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("distance", 100, "time", 10)));

    PhysicalPlan plan =
        eval(
            inputPlan,
            ImmutablePair.of(
                DSL.ref("velocity", INTEGER),
                DSL.divide(DSL.ref("distance", INTEGER), DSL.ref("time", INTEGER))),
            ImmutablePair.of(
                DSL.ref("doubleVelocity", INTEGER),
                DSL.multiply(DSL.ref("velocity", INTEGER), DSL.literal(2))));
    assertThat(
        execute(plan),
        allOf(
            iterableWithSize(1),
            hasItems(
                ExprValueUtils.tupleValue(
                    ImmutableMap.of(
                        "distance", 100, "time", 10, "velocity", 10, "doubleVelocity", 20)))));
  }

  @Test
  public void replace_existing_field() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next())
        .thenReturn(ExprValueUtils.tupleValue(ImmutableMap.of("distance", 100, "time", 10)));

    PhysicalPlan plan =
        eval(
            inputPlan,
            ImmutablePair.of(
                DSL.ref("distance", INTEGER),
                DSL.multiply(DSL.ref("distance", INTEGER), DSL.literal(2))));
    assertThat(
        execute(plan),
        allOf(
            iterableWithSize(1),
            hasItems(ExprValueUtils.tupleValue(ImmutableMap.of("distance", 200, "time", 10)))));
  }

  @Test
  public void do_nothing_with_none_tuple_value() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(ExprValueUtils.integerValue(1));
    PhysicalPlan plan =
        eval(inputPlan, ImmutablePair.of(DSL.ref("response", INTEGER), DSL.ref("referer", STRING)));
    List<ExprValue> result = execute(plan);

    assertThat(result, allOf(iterableWithSize(1), hasItems(ExprValueUtils.integerValue(1))));
  }
}
