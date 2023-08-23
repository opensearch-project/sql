/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.logical;

import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.Aggregator;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalRareTopN;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.logical.LogicalRename;
import org.opensearch.sql.storage.Table;

/** Added for UT coverage */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class LogicalPlanNodeVisitorTest {

  static Expression expression;
  static ReferenceExpression ref;
  static Aggregator aggregator;
  static Table table;

  @BeforeAll
  private static void initMocks() {
    expression = mock(Expression.class);
    ref = mock(ReferenceExpression.class);
    aggregator = mock(Aggregator.class);
    table = mock(Table.class);
  }

  //  @Test
  //  public void logical_plan_should_be_traversable() {
  //    LogicalPlan logicalPlan =
  //        LogicalPlanDSL.rename(
  //            LogicalPlanDSL.aggregation(
  //                LogicalPlanDSL.rareTopN(
  //                    LogicalPlanDSL.filter(LogicalPlanDSL.relation("schema", table), expression),
  //                    CommandType.TOP,
  //                    ImmutableList.of(expression),
  //                    expression),
  //                ImmutableList.of(OpenSearchDSL.named("avg", aggregator)),
  //                ImmutableList.of(OpenSearchDSL.named("group", expression))),
  //            ImmutableMap.of(ref, ref));
  //
  //    Integer result = logicalPlan.accept(new NodesCount(), null);
  //    assertEquals(5, result);
  //  }
  //
  //  @SuppressWarnings("unchecked")
  //  private static Stream<Arguments> getLogicalPlansForVisitorTest() {
  //    LogicalPlan relation = LogicalPlanDSL.relation("schema", table);
  //    LogicalPlan tableScanBuilder =
  //        new TableScanBuilder() {
  //          @Override
  //          public TableScanOperator build() {
  //            return null;
  //          }
  //        };
  //    TableWriteBuilder tableWriteBuilder =
  //        new TableWriteBuilder(null) {
  //          @Override
  //          public TableWriteOperator build(PhysicalPlan child) {
  //            return null;
  //          }
  //        };
  //    LogicalPlan write = LogicalPlanDSL.write(null, table, Collections.emptyList());
  //    LogicalPlan filter = LogicalPlanDSL.filter(relation, expression);
  //    LogicalPlan aggregation =
  //        LogicalPlanDSL.aggregation(
  //            filter,
  //            ImmutableList.of(OpenSearchDSL.named("avg", aggregator)),
  //            ImmutableList.of(OpenSearchDSL.named("group", expression)));
  //
  //
  //    List<Map<String, ReferenceExpression>> nestedArgs =
  //        List.of(
  //            Map.of(
  //                "field", new ReferenceExpression("message.info", STRING),
  //                "path", new ReferenceExpression("message", STRING)));
  //    List<NamedExpression> projectList =
  //        List.of(
  //            new NamedExpression("message.info",
  // OpenSearchDSL.nested(OpenSearchDSL.ref("message.info", STRING)), null));
  //
  //    LogicalNested nested = new LogicalNested(null, nestedArgs, projectList);
  //
  //    LogicalFetchCursor cursor = new LogicalFetchCursor("n:test", mock(StorageEngine.class));
  //
  //    LogicalCloseCursor closeCursor = new LogicalCloseCursor(cursor);
  //
  //    return Stream.of(
  //            relation,
  //            tableScanBuilder,
  //            write,
  //            tableWriteBuilder,
  //            filter,
  //            aggregation,
  //            nested,
  //            cursor,
  //            closeCursor)
  //        .map(Arguments::of);
  //  }
  //
  //  @ParameterizedTest
  //  @MethodSource("getLogicalPlansForVisitorTest")
  //  public void abstract_plan_node_visitor_should_return_null(LogicalPlan plan) {
  //    assertNull(plan.accept(new LogicalPlanNodeVisitor<Integer, Object>() {}, null));
  //  }

  private static class NodesCount extends LogicalPlanNodeVisitor<Integer, Object> {
    @Override
    public Integer visitRelation(LogicalRelation plan, Object context) {
      return 1;
    }

    @Override
    public Integer visitFilter(LogicalFilter plan, Object context) {
      return 1
          + plan.getChild().stream()
              .map(child -> child.accept(this, context))
              .mapToInt(Integer::intValue)
              .sum();
    }

    @Override
    public Integer visitAggregation(LogicalAggregation plan, Object context) {
      return 1
          + plan.getChild().stream()
              .map(child -> child.accept(this, context))
              .mapToInt(Integer::intValue)
              .sum();
    }

    @Override
    public Integer visitRename(LogicalRename plan, Object context) {
      return 1
          + plan.getChild().stream()
              .map(child -> child.accept(this, context))
              .mapToInt(Integer::intValue)
              .sum();
    }

    @Override
    public Integer visitRareTopN(LogicalRareTopN plan, Object context) {
      return 1
          + plan.getChild().stream()
              .map(child -> child.accept(this, context))
              .mapToInt(Integer::intValue)
              .sum();
    }
  }
}
