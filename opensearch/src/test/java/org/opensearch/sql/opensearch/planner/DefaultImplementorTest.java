/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.tree.RareTopN.CommandType;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.AvgAggregator;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.opensearch.expression.OpenSearchDSL;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanDSL;

class DefaultImplementorTest {

  private final DefaultImplementor<Object> implementor = new DefaultImplementor<>();

  @Test
  public void visit_should_return_default_physical_operator() {
    String indexName = "test";
    NamedExpression include = named("age", ref("age", INTEGER));
    ReferenceExpression exclude = ref("name", STRING);
    ReferenceExpression dedupeField = ref("name", STRING);
    Expression filterExpr = literal(ExprBooleanValue.of(true));
    List<NamedExpression> groupByExprs =
        Arrays.asList(OpenSearchDSL.named("age", ref("age", INTEGER)));
    List<Expression> aggExprs = Arrays.asList(ref("age", INTEGER));
    ReferenceExpression rareTopNField = ref("age", INTEGER);
    List<Expression> topByExprs = Arrays.asList(ref("age", INTEGER));
    List<NamedAggregator> aggregators =
        Arrays.asList(
            OpenSearchDSL.named("avg(age)", new AvgAggregator(aggExprs, ExprCoreType.DOUBLE)));
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
        List.of(
            new NamedExpression(
                "message.info",
                OpenSearchDSL.nested(OpenSearchDSL.ref("message.info", STRING)),
                null));
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
}
