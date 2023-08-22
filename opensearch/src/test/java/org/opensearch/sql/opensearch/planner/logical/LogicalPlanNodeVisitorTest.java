/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.logical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.named;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.ast.tree.RareTopN.CommandType;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.Aggregator;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.opensearch.expression.OpenSearchDSL;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalCloseCursor;
import org.opensearch.sql.planner.logical.LogicalFetchCursor;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalNested;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalRareTopN;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.logical.LogicalRename;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.read.TableScanBuilder;
import org.opensearch.sql.storage.write.TableWriteBuilder;
import org.opensearch.sql.storage.write.TableWriteOperator;

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
//            new NamedExpression("message.info", OpenSearchDSL.nested(OpenSearchDSL.ref("message.info", STRING)), null));
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
