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

package org.opensearch.sql.planner;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.logical.LogicalRename;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.planner.physical.AggregationOperator;
import org.opensearch.sql.planner.physical.FilterOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanDSL;
import org.opensearch.sql.planner.physical.PhysicalPlanTestBase;
import org.opensearch.sql.planner.physical.RenameOperator;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

@ExtendWith(MockitoExtension.class)
public class PlannerTest extends PhysicalPlanTestBase {
  @Mock
  private PhysicalPlan scan;

  @Mock
  private StorageEngine storageEngine;

  @Mock
  private LogicalPlanOptimizer optimizer;

  @BeforeEach
  public void setUp() {
    when(storageEngine.getTable(any())).thenReturn(new MockTable());
  }

  @Test
  public void planner_test() {
    doAnswer(returnsFirstArg()).when(optimizer).optimize(any());
    assertPhysicalPlan(
        PhysicalPlanDSL.rename(
            PhysicalPlanDSL.agg(
                PhysicalPlanDSL.filter(
                    scan,
                    dsl.equal(DSL.ref("response", INTEGER), DSL.literal(10))
                ),
                ImmutableList.of(DSL.named("avg(response)", dsl.avg(DSL.ref("response", INTEGER)))),
                ImmutableList.of()
            ),
            ImmutableMap.of(DSL.ref("ivalue", INTEGER), DSL.ref("avg(response)", DOUBLE))
        ),
        LogicalPlanDSL.rename(
            LogicalPlanDSL.aggregation(
                LogicalPlanDSL.filter(
                    LogicalPlanDSL.relation("schema"),
                    dsl.equal(DSL.ref("response", INTEGER), DSL.literal(10))
                ),
                ImmutableList.of(DSL.named("avg(response)", dsl.avg(DSL.ref("response", INTEGER)))),
                ImmutableList.of()
            ),
            ImmutableMap.of(DSL.ref("ivalue", INTEGER), DSL.ref("avg(response)", DOUBLE))
        )
    );
  }

  @Test
  public void plan_a_query_without_relation_involved() {
    // Storage engine mock is not needed here since no relation involved.
    Mockito.reset(storageEngine);

    assertPhysicalPlan(
        PhysicalPlanDSL.project(
            PhysicalPlanDSL.values(emptyList()),
            DSL.named("123", DSL.literal(123)),
            DSL.named("hello", DSL.literal("hello")),
            DSL.named("false", DSL.literal(false))
        ),
        LogicalPlanDSL.project(
            LogicalPlanDSL.values(emptyList()),
            DSL.named("123", DSL.literal(123)),
            DSL.named("hello", DSL.literal("hello")),
            DSL.named("false", DSL.literal(false))
        )
    );
  }

  protected void assertPhysicalPlan(PhysicalPlan expected, LogicalPlan logicalPlan) {
    assertEquals(expected, analyze(logicalPlan));
  }

  protected PhysicalPlan analyze(LogicalPlan logicalPlan) {
    return new Planner(storageEngine, optimizer).plan(logicalPlan);
  }

  protected class MockTable extends LogicalPlanNodeVisitor<PhysicalPlan, Object> implements Table {

    @Override
    public Map<String, ExprType> getFieldTypes() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PhysicalPlan implement(LogicalPlan plan) {
      return plan.accept(this, null);
    }

    @Override
    public PhysicalPlan visitRelation(LogicalRelation plan, Object context) {
      return scan;
    }

    @Override
    public PhysicalPlan visitFilter(LogicalFilter plan, Object context) {
      return new FilterOperator(plan.getChild().get(0).accept(this, context), plan.getCondition());
    }

    @Override
    public PhysicalPlan visitAggregation(LogicalAggregation plan, Object context) {
      return new AggregationOperator(plan.getChild().get(0).accept(this, context),
          plan.getAggregatorList(), plan.getGroupByList()
      );
    }

    @Override
    public PhysicalPlan visitRename(LogicalRename plan, Object context) {
      return new RenameOperator(plan.getChild().get(0).accept(this, context),
          plan.getRenameMap());
    }
  }
}
