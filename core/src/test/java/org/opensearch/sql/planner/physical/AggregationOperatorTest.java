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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;

class AggregationOperatorTest extends PhysicalPlanTestBase {
  @Test
  public void avg_with_one_groups() {
    PhysicalPlan plan = new AggregationOperator(new TestScan(),
        Collections
            .singletonList(DSL.named("avg(response)", dsl.avg(DSL.ref("response", INTEGER)))),
        Collections.singletonList(DSL.named("action", DSL.ref("action", STRING))));
    List<ExprValue> result = execute(plan);
    assertEquals(2, result.size());
    assertThat(result, containsInAnyOrder(
        ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "avg(response)", 268d)),
        ExprValueUtils.tupleValue(ImmutableMap.of("action", "POST", "avg(response)", 350d))
    ));
  }

  @Test
  public void avg_with_two_groups() {
    PhysicalPlan plan = new AggregationOperator(new TestScan(),
        Collections
            .singletonList(DSL.named("avg(response)", dsl.avg(DSL.ref("response", INTEGER)))),
        Arrays.asList(DSL.named("action", DSL.ref("action", STRING)),
            DSL.named("ip", DSL.ref("ip", STRING))));
    List<ExprValue> result = execute(plan);
    assertEquals(3, result.size());
    assertThat(result, containsInAnyOrder(
        ExprValueUtils.tupleValue(
            ImmutableMap.of("action", "GET", "ip", "209.160.24.63", "avg(response)", 302d)),
        ExprValueUtils.tupleValue(
            ImmutableMap.of("action", "GET", "ip", "112.111.162.4", "avg(response)", 200d)),
        ExprValueUtils.tupleValue(
            ImmutableMap.of("action", "POST", "ip", "74.125.19.106", "avg(response)", 350d))
    ));
  }

  @Test
  public void sum_with_one_groups() {
    PhysicalPlan plan = new AggregationOperator(new TestScan(),
        Collections
            .singletonList(DSL.named("sum(response)", dsl.sum(DSL.ref("response", INTEGER)))),
        Collections.singletonList(DSL.named("action", DSL.ref("action", STRING))));
    List<ExprValue> result = execute(plan);
    assertEquals(2, result.size());
    assertThat(result, containsInAnyOrder(
        ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "sum(response)", 804)),
        ExprValueUtils.tupleValue(ImmutableMap.of("action", "POST", "sum(response)", 700))
    ));
  }
}
