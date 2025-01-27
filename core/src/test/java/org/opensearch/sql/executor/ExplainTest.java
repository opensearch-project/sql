/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.ast.tree.RareTopN.CommandType.TOP;
import static org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_ASC;
import static org.opensearch.sql.ast.tree.Trendline.TrendlineType.SMA;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.named;
import static org.opensearch.sql.expression.DSL.ref;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.agg;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.dedupe;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.eval;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.filter;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.limit;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.nested;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.project;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.rareTopN;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.remove;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.rename;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.sort;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.takeOrdered;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.values;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.window;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponseNode;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.planner.physical.FlattenOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.TrendlineOperator;
import org.opensearch.sql.storage.TableScanOperator;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ExplainTest extends ExpressionTestBase {

  private final Explain explain = new Explain();

  private final FakeTableScan tableScan = new FakeTableScan();

  @Test
  void can_explain_project_filter_table_scan() {
    Expression filterExpr =
        DSL.and(
            DSL.equal(ref("balance", INTEGER), literal(10000)),
            DSL.greater(ref("age", INTEGER), literal(30)));
    NamedExpression[] projectList = {
      named("full_name", ref("full_name", STRING), "name"), named("age", ref("age", INTEGER))
    };

    PhysicalPlan plan = project(filter(tableScan, filterExpr), projectList);

    assertEquals(
        new ExplainResponse(
            new ExplainResponseNode(
                "ProjectOperator",
                Map.of("fields", "[name, age]"),
                singletonList(
                    new ExplainResponseNode(
                        "FilterOperator",
                        Map.of("conditions", "and(=(balance, 10000), >(age, 30))"),
                        singletonList(tableScan.explainNode()))))),
        explain.apply(plan));
  }

  @Test
  void can_explain_aggregations() {
    List<Expression> aggExprs = List.of(ref("balance", DOUBLE));
    List<NamedAggregator> aggList =
        List.of(named("avg(balance)", DSL.avg(aggExprs.toArray(new Expression[0]))));
    List<NamedExpression> groupByList = List.of(named("state", ref("state", STRING)));

    PhysicalPlan plan = agg(new FakeTableScan(), aggList, groupByList);
    assertEquals(
        new ExplainResponse(
            new ExplainResponseNode(
                "AggregationOperator",
                Map.of(
                    "aggregators", "[avg(balance)]",
                    "groupBy", "[state]"),
                singletonList(tableScan.explainNode()))),
        explain.apply(plan));
  }

  @Test
  void can_explain_rare_top_n() {
    Expression field = ref("state", STRING);

    PhysicalPlan plan = rareTopN(tableScan, TOP, emptyList(), field);
    assertEquals(
        new ExplainResponse(
            new ExplainResponseNode(
                "RareTopNOperator",
                Map.of("commandType", TOP, "noOfResults", 10, "fields", "[state]", "groupBy", "[]"),
                singletonList(tableScan.explainNode()))),
        explain.apply(plan));
  }

  @Test
  void can_explain_window() {
    List<Expression> partitionByList = List.of(DSL.ref("state", STRING));
    List<Pair<Sort.SortOption, Expression>> sortList =
        List.of(ImmutablePair.of(DEFAULT_ASC, ref("age", INTEGER)));

    PhysicalPlan plan =
        window(tableScan, named(DSL.rank()), new WindowDefinition(partitionByList, sortList));

    assertEquals(
        new ExplainResponse(
            new ExplainResponseNode(
                "WindowOperator",
                Map.of(
                    "function",
                    "rank()",
                    "definition",
                    Map.of(
                        "partitionBy",
                        "[state]",
                        "sortList",
                        Map.of(
                            "age",
                            Map.of(
                                "sortOrder", "ASC",
                                "nullOrder", "NULL_FIRST")))),
                singletonList(tableScan.explainNode()))),
        explain.apply(plan));
  }

  @Test
  void can_explain_other_operators() {
    ReferenceExpression[] removeList = {ref("state", STRING)};
    Map<ReferenceExpression, ReferenceExpression> renameMapping =
        Map.of(ref("state", STRING), ref("s", STRING));
    Pair<ReferenceExpression, Expression> evalExprs =
        ImmutablePair.of(ref("age", INTEGER), DSL.add(ref("age", INTEGER), literal(2)));
    Expression[] dedupeList = {ref("age", INTEGER)};
    Pair<Sort.SortOption, Expression> sortList = ImmutablePair.of(DEFAULT_ASC, ref("age", INTEGER));
    List<LiteralExpression> values = List.of(literal("WA"), literal(30));

    PhysicalPlan plan =
        remove(
            rename(
                eval(dedupe(sort(values(values), sortList), dedupeList), evalExprs), renameMapping),
            removeList);

    assertEquals(
        new ExplainResponse(
            new ExplainResponseNode(
                "RemoveOperator",
                Map.of("removeList", "[state]"),
                singletonList(
                    new ExplainResponseNode(
                        "RenameOperator",
                        Map.of("mapping", Map.of("state", "s")),
                        singletonList(
                            new ExplainResponseNode(
                                "EvalOperator",
                                Map.of("expressions", Map.of("age", "+(age, 2)")),
                                singletonList(
                                    new ExplainResponseNode(
                                        "DedupeOperator",
                                        Map.of(
                                            "dedupeList",
                                            "[age]",
                                            "allowedDuplication",
                                            1,
                                            "keepEmpty",
                                            false,
                                            "consecutive",
                                            false),
                                        singletonList(
                                            new ExplainResponseNode(
                                                "SortOperator",
                                                Map.of(
                                                    "sortList",
                                                    Map.of(
                                                        "age",
                                                        Map.of(
                                                            "sortOrder", "ASC",
                                                            "nullOrder", "NULL_FIRST"))),
                                                singletonList(
                                                    new ExplainResponseNode(
                                                        "ValuesOperator",
                                                        Map.of("values", List.of(values)),
                                                        emptyList())))))))))))),
        explain.apply(plan));
  }

  @Test
  void can_explain_limit() {
    PhysicalPlan plan = limit(tableScan, 10, 5);
    assertEquals(
        new ExplainResponse(
            new ExplainResponseNode(
                "LimitOperator",
                Map.of("limit", 10, "offset", 5),
                singletonList(tableScan.explainNode()))),
        explain.apply(plan));
  }

  @Test
  void can_explain_takeOrdered() {
    Pair<Sort.SortOption, Expression> sort =
        ImmutablePair.of(Sort.SortOption.DEFAULT_ASC, ref("a", INTEGER));
    PhysicalPlan plan = takeOrdered(tableScan, 10, 5, sort);
    assertEquals(
        new ExplainResponse(
            new ExplainResponseNode(
                "TakeOrderedOperator",
                Map.of(
                    "limit",
                    10,
                    "offset",
                    5,
                    "sortList",
                    Map.of("a", Map.of("sortOrder", "ASC", "nullOrder", "NULL_FIRST"))),
                singletonList(tableScan.explainNode()))),
        explain.apply(plan));
  }

  @Test
  void can_explain_nested() {
    Set<String> nestedOperatorArgs = Set.of("message.info", "message");
    Map<String, List<String>> groupedFieldsByPath = Map.of("message", List.of("message.info"));
    PhysicalPlan plan = nested(tableScan, nestedOperatorArgs, groupedFieldsByPath);

    assertEquals(
        new ExplainResponse(
            new ExplainResponseNode(
                "NestedOperator",
                Map.of("nested", Set.of("message.info", "message")),
                singletonList(tableScan.explainNode()))),
        explain.apply(plan));
  }

  @Test
  void can_explain_trendline() {
    PhysicalPlan plan =
        new TrendlineOperator(
            tableScan,
            Arrays.asList(
                Pair.of(
                    AstDSL.computation(2, AstDSL.field("distance"), "distance_alias", SMA), DOUBLE),
                Pair.of(AstDSL.computation(3, AstDSL.field("time"), "time_alias", SMA), DOUBLE)));
    assertEquals(
        new ExplainResponse(
            new ExplainResponseNode(
                "TrendlineOperator",
                ImmutableMap.of(
                    "computations",
                    List.of(
                        ImmutableMap.of(
                            "computationType",
                            "sma",
                            "numberOfDataPoints",
                            "2",
                            "dataField",
                            "distance",
                            "alias",
                            "distance_alias"),
                        ImmutableMap.of(
                            "computationType",
                            "sma",
                            "numberOfDataPoints",
                            "3",
                            "dataField",
                            "time",
                            "alias",
                            "time_alias"))),
                singletonList(tableScan.explainNode()))),
        explain.apply(plan));
  }

  @Test
  void can_explain_flatten() {
    String fieldName = "field_name";
    ReferenceExpression fieldReference = ref(fieldName, STRUCT);

    PhysicalPlan plan = new FlattenOperator(tableScan, fieldReference);
    ExplainResponse actual = explain.apply(plan);

    ExplainResponse expected =
        new ExplainResponse(
            new ExplainResponseNode(
                "FlattenOperator",
                ImmutableMap.of("flattenField", fieldReference),
                singletonList(tableScan.explainNode())));

    assertEquals(expected, actual, "explain flatten");
  }

  private static class FakeTableScan extends TableScanOperator {
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public ExprValue next() {
      return null;
    }

    @Override
    public String toString() {
      return "Fake DSL request";
    }

    /** Used to ignore table scan which is duplicate but required for each operator test. */
    public ExplainResponseNode explainNode() {
      return new ExplainResponseNode(
          "FakeTableScan", Map.of("request", "Fake DSL request"), emptyList());
    }

    public String explain() {
      return "explain";
    }
  }
}
