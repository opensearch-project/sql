/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.calcite.plan.rule.OpenSearchRules;
import org.opensearch.sql.calcite.plan.rule.PPLAggregateConvertRule;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelBuilder;

@ExtendWith(MockitoExtension.class)
public class PPLAggregateConvertRuleTest {
  @Mock VolcanoRuleCall mockedCall;
  @Mock RelNode input;
  @Mock RelOptCluster cluster;
  @Mock RelOptPlanner planner;
  @Mock RelMetadataQuery mq;
  RelDataType type = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
  RelDataType rowType = TYPE_FACTORY.createStructType(List.of(type, type), List.of("a", "b"));
  RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);
  RelBuilder relBuilder;

  @BeforeEach
  public void setUp() throws IllegalAccessException, NoSuchFieldException {
    when(cluster.getTypeFactory()).thenReturn(TYPE_FACTORY);
    when(cluster.getRexBuilder()).thenReturn(rexBuilder);
    when(mq.isVisibleInExplain(any(), any())).thenReturn(true);
    when(cluster.getMetadataQuery()).thenReturn(mq);
    when(cluster.traitSet()).thenReturn(RelTraitSet.createEmpty());
    when(cluster.traitSetOf(Convention.NONE))
        .thenReturn(RelTraitSet.createEmpty().replace(Convention.NONE));
    when(cluster.getPlanner()).thenReturn(planner);
    when(planner.getExecutor()).thenReturn(null);

    when(input.getCluster()).thenReturn(cluster);
    when(input.getRowType()).thenReturn(rowType);
    relBuilder = new OpenSearchRelBuilder(null, cluster, null);
    when(mockedCall.builder()).thenReturn(relBuilder);
  }

  @Test
  public void testRuleMatch() {
    relBuilder.push(input);
    RexNode arg =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            List.of(
                new RexInputRef(0, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT)),
                rexBuilder.makeLiteral(10, type)));
    AggCall aggCall = relBuilder.aggregateCall(SqlStdOperatorTable.SUM, arg);
    LogicalAggregate aggregate =
        (LogicalAggregate)
            relBuilder.aggregate(relBuilder.groupKey(1), ImmutableList.of(aggCall)).build();
    assert (aggregate.getInput() instanceof LogicalProject);
    LogicalProject project = (LogicalProject) aggregate.getInput();

    // Check the predicate in Config
    assertTrue(PPLAggregateConvertRule.Config.containsSumAggCall(aggregate));
    assertTrue(PPLAggregateConvertRule.Config.containsCallWithNumber(project));

    assertEquals(
        "LogicalAggregate(group=[{0}], agg#0=[SUM($1)])\n"
            + "  LogicalProject(b=[$1], $f2=[+($0, 10)])\n",
        aggregate.explain().replaceAll("\\r\\n", "\n"));
    doAnswer(
            invocation -> {
              // Check the final plan
              RelNode rel = invocation.getArgument(0);
              assertTrue(
                  RelOptUtil.areRowTypesEqual(rel.getRowType(), aggregate.getRowType(), false));
              assertEquals(
                  "LogicalProject(b=[$0], $f1=[+($1, *($2, 10))])\n"
                      + "  LogicalAggregate(group=[{0}], null_SUM=[SUM($1)],"
                      + " null_COUNT=[COUNT($1)])\n"
                      + "    LogicalProject(b=[$1], a=[$0])\n",
                  rel.explain().replaceAll("\\r\\n", "\n"));
              return null;
            })
        .when(mockedCall)
        .transformTo(any());
    OpenSearchRules.AGGREGATE_CONVERT_RULE.apply(mockedCall, aggregate, project);
  }
}
