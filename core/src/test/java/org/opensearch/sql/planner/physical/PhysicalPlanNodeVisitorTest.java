/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.opensearch.sql.ast.tree.Trendline.TrendlineType.SMA;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.expression.DSL.named;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.agg;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.dedupe;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.eval;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.filter;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.limit;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.project;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.rareTopN;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.remove;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.rename;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.sort;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.takeOrdered;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.values;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.window;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.opensearch.sql.ast.tree.RareTopN.CommandType;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.window.WindowDefinition;

/** Todo, testing purpose, delete later. */
@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PhysicalPlanNodeVisitorTest extends PhysicalPlanTestBase {
  @Mock PhysicalPlan plan;
  @Mock ReferenceExpression ref;

  @Test
  public void print_physical_plan() {
    PhysicalPlan plan =
        remove(
            project(
                rename(
                    agg(
                        rareTopN(
                            filter(
                                limit(
                                    new TrendlineOperator(
                                        new TestScan(),
                                        Collections.singletonList(
                                            Pair.of(
                                                AstDSL.computation(
                                                    1, AstDSL.field("field"), "alias", SMA),
                                                DOUBLE))),
                                    1,
                                    1),
                                DSL.equal(DSL.ref("response", INTEGER), DSL.literal(10))),
                            CommandType.TOP,
                            ImmutableList.of(),
                            DSL.ref("response", INTEGER)),
                        ImmutableList.of(
                            DSL.named("avg(response)", DSL.avg(DSL.ref("response", INTEGER)))),
                        ImmutableList.of()),
                    ImmutableMap.of(DSL.ref("ivalue", INTEGER), DSL.ref("avg(response)", DOUBLE))),
                named("ref", ref)),
            ref);

    PhysicalPlanPrinter printer = new PhysicalPlanPrinter();
    assertEquals(
        "Remove->\n"
            + "\tProject->\n"
            + "\t\tRename->\n"
            + "\t\t\tAggregation->\n"
            + "\t\t\t\tRareTopN->\n"
            + "\t\t\t\t\tFilter->\n"
            + "\t\t\t\t\t\tLimit->\n"
            + "\t\t\t\t\t\t\tTrendline->",
        printer.print(plan));
  }

  public static Stream<Arguments> getPhysicalPlanForTest() {
    PhysicalPlan plan = mock(PhysicalPlan.class);
    ReferenceExpression ref = mock(ReferenceExpression.class);

    PhysicalPlan filter =
        filter(new TestScan(), DSL.equal(DSL.ref("response", INTEGER), DSL.literal(10)));

    PhysicalPlan aggregation =
        agg(
            filter,
            ImmutableList.of(DSL.named("avg(response)", DSL.avg(DSL.ref("response", INTEGER)))),
            ImmutableList.of());

    PhysicalPlan rename =
        rename(
            aggregation,
            ImmutableMap.of(DSL.ref("ivalue", INTEGER), DSL.ref("avg(response)", DOUBLE)));

    PhysicalPlan project = project(plan, named("ref", ref));

    PhysicalPlan window =
        window(plan, named(DSL.rowNumber()), new WindowDefinition(emptyList(), emptyList()));

    PhysicalPlan remove = remove(plan, ref);

    PhysicalPlan eval = eval(plan, Pair.of(ref, ref));

    PhysicalPlan sort = sort(plan, Pair.of(SortOption.DEFAULT_ASC, ref));

    PhysicalPlan takeOrdered = takeOrdered(plan, 1, 1, Pair.of(SortOption.DEFAULT_ASC, ref));

    PhysicalPlan dedupe = dedupe(plan, ref);

    PhysicalPlan values = values(emptyList());

    PhysicalPlan rareTopN = rareTopN(plan, CommandType.TOP, 5, ImmutableList.of(), ref);

    PhysicalPlan limit = limit(plan, 1, 1);

    Set<String> nestedArgs = Set.of("nested.test");
    Map<String, List<String>> groupedFieldsByPath = Map.of("nested", List.of("nested.test"));
    PhysicalPlan nested = new NestedOperator(plan, nestedArgs, groupedFieldsByPath);

    PhysicalPlan cursorClose = new CursorCloseOperator(plan);

    PhysicalPlan trendline =
        new TrendlineOperator(
            plan,
            Collections.singletonList(
                Pair.of(AstDSL.computation(1, AstDSL.field("field"), "alias", SMA), DOUBLE)));

    return Stream.of(
        Arguments.of(filter, "filter"),
        Arguments.of(aggregation, "aggregation"),
        Arguments.of(rename, "rename"),
        Arguments.of(project, "project"),
        Arguments.of(window, "window"),
        Arguments.of(remove, "remove"),
        Arguments.of(eval, "eval"),
        Arguments.of(sort, "sort"),
        Arguments.of(takeOrdered, "takeOrdered"),
        Arguments.of(dedupe, "dedupe"),
        Arguments.of(values, "values"),
        Arguments.of(rareTopN, "rareTopN"),
        Arguments.of(limit, "limit"),
        Arguments.of(nested, "nested"),
        Arguments.of(cursorClose, "cursorClose"),
        Arguments.of(trendline, "trendline"));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("getPhysicalPlanForTest")
  public void test_PhysicalPlanVisitor_should_return_null(PhysicalPlan plan, String name) {
    assertNull(plan.accept(new PhysicalPlanNodeVisitor<Integer, Object>() {}, null));
  }

  @Test
  public void test_visitMLCommons() {
    PhysicalPlanNodeVisitor physicalPlanNodeVisitor =
        new PhysicalPlanNodeVisitor<Integer, Object>() {};

    assertNull(physicalPlanNodeVisitor.visitMLCommons(plan, null));
  }

  @Test
  public void test_visitAD() {
    PhysicalPlanNodeVisitor physicalPlanNodeVisitor =
        new PhysicalPlanNodeVisitor<Integer, Object>() {};

    assertNull(physicalPlanNodeVisitor.visitAD(plan, null));
  }

  @Test
  public void test_visitML() {
    PhysicalPlanNodeVisitor physicalPlanNodeVisitor =
        new PhysicalPlanNodeVisitor<Integer, Object>() {};

    assertNull(physicalPlanNodeVisitor.visitML(plan, null));
  }

  public static class PhysicalPlanPrinter extends PhysicalPlanNodeVisitor<String, Integer> {

    public String print(PhysicalPlan node) {
      return node.accept(this, 0);
    }

    @Override
    public String visitFilter(FilterOperator node, Integer tabs) {
      return name(node, "Filter->", tabs);
    }

    @Override
    public String visitAggregation(AggregationOperator node, Integer tabs) {
      return name(node, "Aggregation->", tabs);
    }

    @Override
    public String visitRename(RenameOperator node, Integer tabs) {
      return name(node, "Rename->", tabs);
    }

    @Override
    public String visitProject(ProjectOperator node, Integer tabs) {
      return name(node, "Project->", tabs);
    }

    @Override
    public String visitRemove(RemoveOperator node, Integer tabs) {
      return name(node, "Remove->", tabs);
    }

    @Override
    public String visitRareTopN(RareTopNOperator node, Integer tabs) {
      return name(node, "RareTopN->", tabs);
    }

    @Override
    public String visitLimit(LimitOperator node, Integer tabs) {
      return name(node, "Limit->", tabs);
    }

    @Override
    public String visitTrendline(TrendlineOperator node, Integer tabs) {
      return name(node, "Trendline->", tabs);
    }

    private String name(PhysicalPlan node, String current, int tabs) {
      String child = node.getChild().get(0).accept(this, tabs + 1);
      StringBuilder sb = new StringBuilder();
      for (Integer i = 0; i < tabs; i++) {
        sb.append("\t");
      }
      sb.append(current);
      if (!Strings.isNullOrEmpty(child)) {
        sb.append("\n");
        sb.append(child);
      }
      return sb.toString();
    }
  }
}
