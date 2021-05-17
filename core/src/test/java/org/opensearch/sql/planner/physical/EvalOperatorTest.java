/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
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
  @Mock
  private PhysicalPlan inputPlan;

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
                dsl.divide(DSL.ref("distance", INTEGER), DSL.ref("time", INTEGER))));
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
                DSL.ref("velocity", DOUBLE), dsl.divide(DSL.ref("distance", INTEGER), DSL.ref(
                    "time", INTEGER))),
            ImmutablePair.of(
                DSL.ref("doubleDistance", INTEGER),
                dsl.multiply(DSL.ref("distance", INTEGER), DSL.literal(2))));
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
                DSL.ref("velocity", INTEGER), dsl.divide(DSL.ref("distance", INTEGER), DSL.ref(
                    "time", INTEGER))),
            ImmutablePair.of(
                DSL.ref("doubleVelocity", INTEGER),
                dsl.multiply(DSL.ref("velocity", INTEGER), DSL.literal(2))));
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
                dsl.multiply(DSL.ref("distance", INTEGER), DSL.literal(2))));
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
    PhysicalPlan plan = eval(inputPlan, ImmutablePair.of(DSL.ref("response", INTEGER),
        DSL.ref("referer", STRING)));
    List<ExprValue> result = execute(plan);

    assertThat(result, allOf(iterableWithSize(1), hasItems(ExprValueUtils.integerValue(1))));
  }
}
