/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.executor;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
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
import org.opensearch.sql.opensearch.executor.protector.OpenSearchExecutionProtector;
import org.opensearch.sql.opensearch.executor.protector.ResourceMonitorPlan;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.opensearch.storage.OpenSearchIndexScan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanDSL;

@ExtendWith(MockitoExtension.class)
class OpenSearchExecutionProtectorTest {

  @Mock
  private OpenSearchClient client;

  @Mock
  private ResourceMonitor resourceMonitor;

  @Mock
  private OpenSearchExprValueFactory exprValueFactory;

  @Mock
  private OpenSearchSettings settings;

  private OpenSearchExecutionProtector executionProtector;

  @BeforeEach
  public void setup() {
    executionProtector = new OpenSearchExecutionProtector(resourceMonitor);
  }

  @Test
  public void testProtectIndexScan() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);

    String indexName = "test";
    NamedExpression include = named("age", ref("age", INTEGER));
    ReferenceExpression exclude = ref("name", STRING);
    ReferenceExpression dedupeField = ref("name", STRING);
    ReferenceExpression topField = ref("name", STRING);
    List<Expression> topExprs = Arrays.asList(ref("age", INTEGER));
    Expression filterExpr = literal(ExprBooleanValue.of(true));
    List<NamedExpression> groupByExprs = Arrays.asList(named("age", ref("age", INTEGER)));
    List<NamedAggregator> aggregators =
        Arrays.asList(named("avg(age)", new AvgAggregator(Arrays.asList(ref("age", INTEGER)),
            DOUBLE)));
    Map<ReferenceExpression, ReferenceExpression> mappings =
        ImmutableMap.of(ref("name", STRING), ref("lastname", STRING));
    Pair<ReferenceExpression, Expression> newEvalField =
        ImmutablePair.of(ref("name1", STRING), ref("name", STRING));
    Integer sortCount = 100;
    Pair<Sort.SortOption, Expression> sortField =
        ImmutablePair.of(DEFAULT_ASC, ref("name1", STRING));
    Integer size = 200;
    Integer limit = 10;
    Integer offset = 10;
    ReferenceExpression parseField = ref("name", STRING);
    String parsePattern = "(?<firstName>\\w+) (?<lastName>\\w+)";
    Map<String, String> parseGroups = ImmutableMap.of("firstName", "", "lastName", "");

    assertEquals(
        PhysicalPlanDSL.project(
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
                                                    PhysicalPlanDSL.parse(
                                                        resourceMonitor(
                                                            new OpenSearchIndexScan(
                                                                client, settings, indexName,
                                                                exprValueFactory)),
                                                        parseField,
                                                        parsePattern,
                                                        parseGroups),
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
            include),
        executionProtector.protect(
            PhysicalPlanDSL.project(
                PhysicalPlanDSL.limit(
                    PhysicalPlanDSL.dedupe(
                        PhysicalPlanDSL.rareTopN(
                            PhysicalPlanDSL.sort(
                                PhysicalPlanDSL.eval(
                                    PhysicalPlanDSL.remove(
                                        PhysicalPlanDSL.rename(
                                            PhysicalPlanDSL.agg(
                                                filter(
                                                    PhysicalPlanDSL.parse(
                                                        new OpenSearchIndexScan(
                                                            client, settings, indexName,
                                                            exprValueFactory),
                                                        parseField,
                                                        parsePattern,
                                                        parseGroups),
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
                include)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testProtectSortForWindowOperator() {
    NamedExpression rank = named(mock(RankFunction.class));
    Pair<Sort.SortOption, Expression> sortItem =
        ImmutablePair.of(DEFAULT_ASC, DSL.ref("age", INTEGER));
    WindowDefinition windowDefinition =
        new WindowDefinition(emptyList(), ImmutableList.of(sortItem));

    assertEquals(
        window(
            resourceMonitor(
                sort(
                    values(emptyList()),
                    sortItem)),
            rank,
            windowDefinition),
        executionProtector.protect(
            window(
                sort(
                    values(emptyList()),
                    sortItem
                ),
                rank,
                windowDefinition)));
  }

  @Test
  public void testProtectWindowOperatorInput() {
    NamedExpression avg = named(mock(AggregateWindowFunction.class));
    WindowDefinition windowDefinition = mock(WindowDefinition.class);

    assertEquals(
        window(
            resourceMonitor(
                values()),
            avg,
            windowDefinition),
        executionProtector.protect(
            window(
                values(),
                avg,
                windowDefinition)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testNotProtectWindowOperatorInputIfAlreadyProtected() {
    NamedExpression avg = named(mock(AggregateWindowFunction.class));
    Pair<Sort.SortOption, Expression> sortItem =
        ImmutablePair.of(DEFAULT_ASC, DSL.ref("age", INTEGER));
    WindowDefinition windowDefinition =
        new WindowDefinition(emptyList(), ImmutableList.of(sortItem));

    assertEquals(
        window(
            resourceMonitor(
                sort(
                    values(emptyList()),
                    sortItem)),
            avg,
            windowDefinition),
        executionProtector.protect(
            window(
                sort(
                    values(emptyList()),
                    sortItem),
                avg,
                windowDefinition)));
  }

  @Test
  public void testWithoutProtection() {
    Expression filterExpr = literal(ExprBooleanValue.of(true));

    assertEquals(
        filter(
            filter(null, filterExpr),
            filterExpr),
        executionProtector.protect(
            filter(
                filter(null, filterExpr),
                filterExpr)
        )
    );
  }

  PhysicalPlan resourceMonitor(PhysicalPlan input) {
    return new ResourceMonitorPlan(input, resourceMonitor);
  }
}
