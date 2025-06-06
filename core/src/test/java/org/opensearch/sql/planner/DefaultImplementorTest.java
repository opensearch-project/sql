/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.ast.tree.Trendline.TrendlineType.SMA;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.named;
import static org.opensearch.sql.expression.DSL.ref;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.aggregation;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.eval;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.filter;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.limit;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.nested;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.project;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.rareTopN;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.remove;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.rename;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.sort;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.values;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.window;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.tree.RareTopN.CommandType;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.AvgAggregator;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.expression.window.ranking.RowNumberFunction;
import org.opensearch.sql.planner.logical.LogicalCloseCursor;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.logical.LogicalTrendline;
import org.opensearch.sql.planner.logical.LogicalValues;
import org.opensearch.sql.planner.physical.CursorCloseOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanDSL;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.planner.physical.TrendlineOperator;
import org.opensearch.sql.planner.physical.ValuesOperator;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.read.TableScanBuilder;
import org.opensearch.sql.storage.write.TableWriteBuilder;
import org.opensearch.sql.storage.write.TableWriteOperator;
import org.opensearch.sql.utils.TestOperator;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DefaultImplementorTest {

  @Mock private Table table;

  private final DefaultImplementor<Object> implementor = new DefaultImplementor<>();

  @Test
  public void visit_should_return_default_physical_operator() {
    String indexName = "test";
    NamedExpression include = named("age", ref("age", INTEGER));
    ReferenceExpression exclude = ref("name", STRING);
    ReferenceExpression dedupeField = ref("name", STRING);
    Expression filterExpr = literal(ExprBooleanValue.of(true));
    List<NamedExpression> groupByExprs = Arrays.asList(DSL.named("age", ref("age", INTEGER)));
    List<Expression> aggExprs = Arrays.asList(ref("age", INTEGER));
    ReferenceExpression rareTopNField = ref("age", INTEGER);
    List<Expression> topByExprs = Arrays.asList(ref("age", INTEGER));
    List<NamedAggregator> aggregators =
        Arrays.asList(DSL.named("avg(age)", new AvgAggregator(aggExprs, ExprCoreType.DOUBLE)));
    Map<ReferenceExpression, ReferenceExpression> mappings =
        ImmutableMap.of(ref("name", STRING), ref("lastname", STRING));
    Pair<ReferenceExpression, Expression> newEvalField =
        ImmutablePair.of(ref("name1", STRING), ref("name", STRING));
    Pair<Sort.SortOption, Expression> sortField =
        ImmutablePair.of(Sort.SortOption.DEFAULT_ASC, ref("name1", STRING));
    Integer limit = 1;
    Integer offset = 1;
    List<Map<String, ReferenceExpression>> nestedArgs =
        List.of(
            Map.of(
                "field", new ReferenceExpression("message.info", STRING),
                "path", new ReferenceExpression("message", STRING)));
    List<NamedExpression> nestedProjectList =
        List.of(new NamedExpression("message.info", DSL.nested(DSL.ref("message.info", STRING))));
    Set<String> nestedOperatorArgs = Set.of("message.info");
    Map<String, List<String>> groupedFieldsByPath = Map.of("message", List.of("message.info"));

    LogicalPlan plan =
        project(
            nested(
                limit(
                    LogicalPlanDSL.dedupe(
                        rareTopN(
                            sort(
                                eval(
                                    remove(
                                        rename(
                                            aggregation(
                                                filter(values(emptyList()), filterExpr),
                                                aggregators,
                                                groupByExprs),
                                            mappings),
                                        exclude),
                                    newEvalField),
                                sortField),
                            CommandType.TOP,
                            topByExprs,
                            rareTopNField),
                        dedupeField),
                    limit,
                    offset),
                nestedArgs,
                nestedProjectList),
            include);

    PhysicalPlan actual = plan.accept(implementor, null);

    assertEquals(
        PhysicalPlanDSL.project(
            PhysicalPlanDSL.nested(
                PhysicalPlanDSL.limit(
                    PhysicalPlanDSL.dedupe(
                        PhysicalPlanDSL.rareTopN(
                            PhysicalPlanDSL.sort(
                                PhysicalPlanDSL.eval(
                                    PhysicalPlanDSL.remove(
                                        PhysicalPlanDSL.rename(
                                            PhysicalPlanDSL.agg(
                                                PhysicalPlanDSL.filter(
                                                    PhysicalPlanDSL.values(emptyList()),
                                                    filterExpr),
                                                aggregators,
                                                groupByExprs),
                                            mappings),
                                        exclude),
                                    newEvalField),
                                sortField),
                            CommandType.TOP,
                            topByExprs,
                            rareTopNField),
                        dedupeField),
                    limit,
                    offset),
                nestedOperatorArgs,
                groupedFieldsByPath),
            include),
        actual);
  }

  @Test
  public void visitRelation_should_throw_an_exception() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> new LogicalRelation("test", table).accept(implementor, null));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void visitWindowOperator_should_return_PhysicalWindowOperator() {
    NamedExpression windowFunction = named(new RowNumberFunction());
    WindowDefinition windowDefinition =
        new WindowDefinition(
            Collections.singletonList(ref("state", STRING)),
            Collections.singletonList(
                ImmutablePair.of(Sort.SortOption.DEFAULT_DESC, ref("age", INTEGER))));

    NamedExpression[] projectList = {
      named("state", ref("state", STRING)), named("row_number", ref("row_number", INTEGER))
    };
    Pair[] sortList = {
      ImmutablePair.of(Sort.SortOption.DEFAULT_ASC, ref("state", STRING)),
      ImmutablePair.of(Sort.SortOption.DEFAULT_DESC, ref("age", STRING))
    };

    LogicalPlan logicalPlan =
        project(window(sort(values(), sortList), windowFunction, windowDefinition), projectList);

    PhysicalPlan physicalPlan =
        PhysicalPlanDSL.project(
            PhysicalPlanDSL.window(
                PhysicalPlanDSL.sort(PhysicalPlanDSL.values(), sortList),
                windowFunction,
                windowDefinition),
            projectList);

    assertEquals(physicalPlan, logicalPlan.accept(implementor, null));
  }

  @Test
  void visitLogicalCursor_deserializes_it() {
    var engine = mock(StorageEngine.class);

    var physicalPlan = new TestOperator();
    var logicalPlan =
        LogicalPlanDSL.fetchCursor(
            new PlanSerializer(engine).convertToCursor(physicalPlan).toString(), engine);
    assertEquals(physicalPlan, logicalPlan.accept(implementor, null));
  }

  @Test
  public void visitTableScanBuilder_should_build_TableScanOperator() {
    TableScanOperator tableScanOperator = mock(TableScanOperator.class);
    TableScanBuilder tableScanBuilder =
        new TableScanBuilder() {
          @Override
          public TableScanOperator build() {
            return tableScanOperator;
          }
        };
    assertEquals(tableScanOperator, tableScanBuilder.accept(implementor, null));
  }

  @Test
  public void visitTableWriteBuilder_should_build_TableWriteOperator() {
    LogicalPlan child = values();
    TableWriteOperator tableWriteOperator = mock(TableWriteOperator.class);
    TableWriteBuilder logicalPlan =
        new TableWriteBuilder(child) {
          @Override
          public TableWriteOperator build(PhysicalPlan child) {
            return tableWriteOperator;
          }
        };
    assertEquals(tableWriteOperator, logicalPlan.accept(implementor, null));
  }

  @Test
  public void visitCloseCursor_should_build_CursorCloseOperator() {
    var logicalChild = mock(LogicalPlan.class);
    var physicalChild = mock(PhysicalPlan.class);
    when(logicalChild.accept(implementor, null)).thenReturn(physicalChild);
    var logicalPlan = new LogicalCloseCursor(logicalChild);
    var implemented = logicalPlan.accept(implementor, null);
    assertTrue(implemented instanceof CursorCloseOperator);
    assertSame(physicalChild, implemented.getChild().get(0));
  }

  @Test
  public void visitPaginate_should_remove_it_from_tree() {
    var logicalPlanTree =
        new LogicalPaginate(
            42,
            List.of(
                new LogicalProject(new LogicalValues(List.of(List.of())), List.of(), List.of())));
    var physicalPlanTree =
        new ProjectOperator(new ValuesOperator(List.of(List.of())), List.of(), List.of());
    assertEquals(physicalPlanTree, logicalPlanTree.accept(implementor, null));
  }

  @Test
  public void visitLimit_support_return_takeOrdered() {
    // replace SortOperator + LimitOperator with TakeOrderedOperator
    Pair<Sort.SortOption, Expression> sort =
        ImmutablePair.of(Sort.SortOption.DEFAULT_ASC, ref("a", INTEGER));
    var logicalValues = values(emptyList());
    var logicalSort = sort(logicalValues, sort);
    var logicalLimit = limit(logicalSort, 10, 5);
    PhysicalPlan physicalPlanTree =
        PhysicalPlanDSL.takeOrdered(PhysicalPlanDSL.values(emptyList()), 10, 5, sort);
    assertEquals(physicalPlanTree, logicalLimit.accept(implementor, null));

    // don't replace if LimitOperator's child is not SortOperator
    Pair<ReferenceExpression, Expression> newEvalField =
        ImmutablePair.of(ref("name1", STRING), ref("name", STRING));
    var logicalEval = eval(logicalSort, newEvalField);
    logicalLimit = limit(logicalEval, 10, 5);
    physicalPlanTree =
        PhysicalPlanDSL.limit(
            PhysicalPlanDSL.eval(
                PhysicalPlanDSL.sort(PhysicalPlanDSL.values(emptyList()), sort), newEvalField),
            10,
            5);
    assertEquals(physicalPlanTree, logicalLimit.accept(implementor, null));
  }

  @Test
  public void visitTrendline_should_build_TrendlineOperator() {
    var logicalChild = mock(LogicalPlan.class);
    var physicalChild = mock(PhysicalPlan.class);
    when(logicalChild.accept(implementor, null)).thenReturn(physicalChild);
    final Trendline.TrendlineComputation computation =
        AstDSL.computation(1, AstDSL.field("field"), "alias", SMA);
    var logicalPlan =
        new LogicalTrendline(
            logicalChild, Collections.singletonList(Pair.of(computation, ExprCoreType.DOUBLE)));
    var implemented = logicalPlan.accept(implementor, null);
    assertInstanceOf(TrendlineOperator.class, implemented);
    assertSame(physicalChild, implemented.getChild().get(0));
  }
}
