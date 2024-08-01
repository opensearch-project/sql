/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.protector;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_ASC;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.named;
import static org.opensearch.sql.expression.DSL.ref;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.filter;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.sort;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.values;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.window;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.node.NodeClient;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.RareTopN.CommandType;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.AvgAggregator;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.expression.window.aggregation.AggregateWindowFunction;
import org.opensearch.sql.expression.window.ranking.RankFunction;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.planner.physical.ADOperator;
import org.opensearch.sql.opensearch.planner.physical.MLCommonsOperator;
import org.opensearch.sql.opensearch.planner.physical.MLOperator;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScan;
import org.opensearch.sql.planner.physical.CursorCloseOperator;
import org.opensearch.sql.planner.physical.NestedOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanDSL;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OpenSearchExecutionProtectorTest {

  @Mock private OpenSearchClient client;

  @Mock private ResourceMonitor resourceMonitor;

  @Mock private OpenSearchExprValueFactory exprValueFactory;

  @Mock private OpenSearchSettings settings;

  @Mock private BiFunction<String, Map<String, Object>, Map<String, Object>> lookupFunction;

  private OpenSearchExecutionProtector executionProtector;

  @BeforeEach
  public void setup() {
    executionProtector = new OpenSearchExecutionProtector(resourceMonitor);
  }

  @Test
  void test_protect_indexScan() {
    String indexName = "test";
    final int maxResultWindow = 10000;
    final int querySizeLimit = 200;
    NamedExpression include = named("age", ref("age", INTEGER));
    ReferenceExpression exclude = ref("name", STRING);
    ReferenceExpression dedupeField = ref("name", STRING);
    ReferenceExpression topField = ref("name", STRING);
    List<Expression> topExprs = List.of(ref("age", INTEGER));
    Expression filterExpr = literal(ExprBooleanValue.of(true));
    List<NamedExpression> groupByExprs = List.of(named("age", ref("age", INTEGER)));
    List<NamedAggregator> aggregators =
        List.of(named("avg(age)", new AvgAggregator(List.of(ref("age", INTEGER)), DOUBLE)));
    Map<ReferenceExpression, ReferenceExpression> mappings =
        ImmutableMap.of(ref("name", STRING), ref("lastname", STRING));
    Pair<ReferenceExpression, Expression> newEvalField =
        ImmutablePair.of(ref("name1", STRING), ref("name", STRING));
    Pair<Sort.SortOption, Expression> sortField =
        ImmutablePair.of(DEFAULT_ASC, ref("name1", STRING));
    Integer limit = 10;
    Integer offset = 10;

    final var name = new OpenSearchRequest.IndexName(indexName);
    final var request =
        new OpenSearchRequestBuilder(querySizeLimit, exprValueFactory)
            .build(
                name,
                maxResultWindow,
                settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE));
    assertEquals(
        PhysicalPlanDSL.project(
            PhysicalPlanDSL.lookup(
                PhysicalPlanDSL.limit(
                    PhysicalPlanDSL.dedupe(
                        PhysicalPlanDSL.rareTopN(
                            resourceMonitor(
                                PhysicalPlanDSL.sort(
                                    PhysicalPlanDSL.eval(
                                        PhysicalPlanDSL.remove(
                                            PhysicalPlanDSL.rename(
                                                PhysicalPlanDSL.agg(
                                                    filter(
                                                        resourceMonitor(
                                                            new OpenSearchIndexScan(
                                                                client, maxResultWindow, request)),
                                                        filterExpr),
                                                    aggregators,
                                                    groupByExprs),
                                                mappings),
                                            exclude),
                                        newEvalField),
                                    sortField)),
                            CommandType.TOP,
                            topExprs,
                            topField),
                        dedupeField),
                    limit,
                    offset),
                "lookup_index_name",
                Map.of(),
                false,
                Map.of(),
                lookupFunction),
            include),
        executionProtector.protect(
            PhysicalPlanDSL.project(
                PhysicalPlanDSL.lookup(
                    PhysicalPlanDSL.limit(
                        PhysicalPlanDSL.dedupe(
                            PhysicalPlanDSL.rareTopN(
                                PhysicalPlanDSL.sort(
                                    PhysicalPlanDSL.eval(
                                        PhysicalPlanDSL.remove(
                                            PhysicalPlanDSL.rename(
                                                PhysicalPlanDSL.agg(
                                                    filter(
                                                        new OpenSearchIndexScan(
                                                            client, maxResultWindow, request),
                                                        filterExpr),
                                                    aggregators,
                                                    groupByExprs),
                                                mappings),
                                            exclude),
                                        newEvalField),
                                    sortField),
                                CommandType.TOP,
                                topExprs,
                                topField),
                            dedupeField),
                        limit,
                        offset),
                    "lookup_index_name",
                    Map.of(),
                    false,
                    Map.of(),
                    lookupFunction),
                include)));
  }

  @SuppressWarnings("unchecked")
  @Test
  void test_protect_sort_for_windowOperator() {
    NamedExpression rank = named(mock(RankFunction.class));
    Pair<Sort.SortOption, Expression> sortItem =
        ImmutablePair.of(DEFAULT_ASC, DSL.ref("age", INTEGER));
    WindowDefinition windowDefinition =
        new WindowDefinition(emptyList(), ImmutableList.of(sortItem));

    assertEquals(
        window(resourceMonitor(sort(values(emptyList()), sortItem)), rank, windowDefinition),
        executionProtector.protect(
            window(sort(values(emptyList()), sortItem), rank, windowDefinition)));
  }

  @Test
  void test_protect_windowOperator_input() {
    NamedExpression avg = named(mock(AggregateWindowFunction.class));
    WindowDefinition windowDefinition = mock(WindowDefinition.class);

    assertEquals(
        window(resourceMonitor(values()), avg, windowDefinition),
        executionProtector.protect(window(values(), avg, windowDefinition)));
  }

  @SuppressWarnings("unchecked")
  @Test
  void test_not_protect_windowOperator_input_if_already_protected() {
    NamedExpression avg = named(mock(AggregateWindowFunction.class));
    Pair<Sort.SortOption, Expression> sortItem =
        ImmutablePair.of(DEFAULT_ASC, DSL.ref("age", INTEGER));
    WindowDefinition windowDefinition =
        new WindowDefinition(emptyList(), ImmutableList.of(sortItem));

    assertEquals(
        window(resourceMonitor(sort(values(emptyList()), sortItem)), avg, windowDefinition),
        executionProtector.protect(
            window(sort(values(emptyList()), sortItem), avg, windowDefinition)));
  }

  @Test
  void test_without_protection() {
    Expression filterExpr = literal(ExprBooleanValue.of(true));

    assertEquals(
        filter(filter(null, filterExpr), filterExpr),
        executionProtector.protect(filter(filter(null, filterExpr), filterExpr)));
  }

  @Test
  void test_visitMLcommons() {
    NodeClient nodeClient = mock(NodeClient.class);
    MLCommonsOperator mlCommonsOperator =
        new MLCommonsOperator(
            values(emptyList()),
            "kmeans",
            new HashMap<String, Literal>() {
              {
                put("centroids", new Literal(3, DataType.INTEGER));
                put("iterations", new Literal(2, DataType.INTEGER));
                put("distance_type", new Literal(null, DataType.STRING));
              }
            },
            nodeClient);

    assertEquals(
        executionProtector.doProtect(mlCommonsOperator),
        executionProtector.visitMLCommons(mlCommonsOperator, null));
  }

  @Test
  void test_visitAD() {
    NodeClient nodeClient = mock(NodeClient.class);
    ADOperator adOperator =
        new ADOperator(
            values(emptyList()),
            new HashMap<String, Literal>() {
              {
                put("shingle_size", new Literal(8, DataType.INTEGER));
                put("time_decay", new Literal(0.0001, DataType.DOUBLE));
                put("time_field", new Literal(null, DataType.STRING));
              }
            },
            nodeClient);

    assertEquals(
        executionProtector.doProtect(adOperator), executionProtector.visitAD(adOperator, null));
  }

  @Test
  void test_visitML() {
    NodeClient nodeClient = mock(NodeClient.class);
    MLOperator mlOperator =
        new MLOperator(
            values(emptyList()),
            new HashMap<String, Literal>() {
              {
                put("action", new Literal("train", DataType.STRING));
                put("algorithm", new Literal("rcf", DataType.STRING));
                put("shingle_size", new Literal(8, DataType.INTEGER));
                put("time_decay", new Literal(0.0001, DataType.DOUBLE));
                put("time_field", new Literal(null, DataType.STRING));
              }
            },
            nodeClient);

    assertEquals(
        executionProtector.doProtect(mlOperator), executionProtector.visitML(mlOperator, null));
  }

  @Test
  void test_visitNested() {
    Set<String> args = Set.of("message.info");
    Map<String, List<String>> groupedFieldsByPath = Map.of("message", List.of("message.info"));
    NestedOperator nestedOperator =
        new NestedOperator(values(emptyList()), args, groupedFieldsByPath);

    assertEquals(
        executionProtector.doProtect(nestedOperator),
        executionProtector.visitNested(nestedOperator, values(emptyList())));
  }

  @Test
  void do_nothing_with_CursorCloseOperator_and_children() {
    var child = mock(PhysicalPlan.class);
    var plan = new CursorCloseOperator(child);
    assertSame(plan, executionProtector.protect(plan));
    verify(child, never()).accept(executionProtector, null);
  }

  PhysicalPlan resourceMonitor(PhysicalPlan input) {
    return new ResourceMonitorPlan(input, resourceMonitor);
  }
}
