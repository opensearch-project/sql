/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

@ExtendWith(MockitoExtension.class)
public class CalciteIndexScanCostTest {
  static final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  final RexBuilder builder = new RexBuilder(typeFactory);

  @Mock private static RelOptCluster cluster;
  @Mock private static RelOptTable table;
  @Mock private static OpenSearchIndex osIndex;
  @Mock private static RelOptPlanner planner;
  @Mock private static RelMetadataQuery mq;

  @BeforeEach
  void setUp() {
    RelTraitSet traitSet = mock(RelTraitSet.class);
    when(cluster.traitSetOf(any(Convention.class))).thenReturn(traitSet);
    when(osIndex.getMaxResultWindow()).thenReturn(10000);
    Settings settings = mock(Settings.class);
    when(settings.getSettingValue(Key.CALCITE_PUSHDOWN_ROWCOUNT_ESTIMATION_FACTOR)).thenReturn(0.9);
    when(osIndex.getSettings()).thenReturn(settings);

    RelOptCostFactory costFactory = mock(RelOptCostFactory.class);
    when(planner.getCostFactory()).thenReturn(costFactory);
    when(costFactory.makeCost(anyDouble(), anyDouble(), anyDouble()))
        .thenAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              RelOptCost optCost = mock(RelOptCost.class);
              when(optCost.getRows()).thenReturn((Double) args[0]);
              return optCost;
            });
  }

  @Test
  void test_cost_on_non_pushdown() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(table.getRowType()).thenReturn(relDataType);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);
    assertEquals(90000, scan.computeSelfCost(planner, mq).getRows());
  }

  @Test
  void test_cost_on_project_pushdown() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(table.getRowType()).thenReturn(relDataType);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    List<String> projectDigest = List.of("A");
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.PROJECT, projectDigest, (OSRequestBuilderAction) req -> {}));
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(projectDigest.size()));
    assertEquals(9000, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());

    projectDigest = List.of("A", "B", "C");
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.PROJECT, projectDigest, (OSRequestBuilderAction) req -> {}));
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(projectDigest.size()));
    assertEquals(27000, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_limit_pushdown() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    LimitDigest limitDigest = new LimitDigest(100, 0);
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.LIMIT, limitDigest, (OSRequestBuilderAction) req -> {}));
    assertEquals(891, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_filter_pushdown() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    RexNode condition =
        builder.makeCall(
            SqlStdOperatorTable.EQUALS,
            builder.makeInputRef(scan, 0),
            builder.makeLiteral("Hello"));
    FilterDigest filterDigest = new FilterDigest(0, condition);
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.FILTER, filterDigest, (OSRequestBuilderAction) req -> {}));
    assertEquals(13500, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_filter_script_pushdown() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    RexNode condition =
        builder.makeCall(
            SqlStdOperatorTable.EQUALS,
            builder.makeInputRef(scan, 0),
            builder.makeLiteral("Hello"));
    FilterDigest filterDigest = new FilterDigest(1, condition);
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.SCRIPT, filterDigest, (OSRequestBuilderAction) req -> {}));
    assertEquals(14985, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_sort_pushdown() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);
    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.SORT, null, (OSRequestBuilderAction) req -> {}));
    assertEquals(99000, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_collapse_pushdown() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);
    scan.getPushDownContext()
        .add(
            new PushDownOperation(PushDownType.COLLAPSE, null, (OSRequestBuilderAction) req -> {}));
    assertEquals(9900, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_aggregate_pushdown() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(relDataType.getFieldCount()).thenReturn(10);
    lenient().when(cluster.getTypeFactory()).thenReturn(typeFactory);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);
    Aggregate aggregate =
        new LogicalAggregate(
            cluster,
            cluster.traitSetOf(Convention.NONE),
            List.of(),
            scan,
            ImmutableBitSet.of(0),
            null,
            List.of());
    when(mq.getRowCount(aggregate)).thenReturn(1000d);
    AggPushDownAction action =
        new AggPushDownAction(Pair.of(List.of(), null), null, List.of()) {
          @Override
          public void apply(OpenSearchRequestBuilder requestBuilder) {}
        };
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(1));
    lenient().when(relDataType.getFieldCount()).thenReturn(1);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.AGGREGATION, aggregate, action));
    assertEquals(1800, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_aggregate_pushdown_with_one_aggCall() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(relDataType.getFieldCount()).thenReturn(10);
    lenient().when(cluster.getTypeFactory()).thenReturn(typeFactory);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);
    AggPushDownAction action =
        new AggPushDownAction(Pair.of(List.of(), null), null, List.of()) {
          @Override
          public void apply(OpenSearchRequestBuilder requestBuilder) {}
        };
    AggregateCall countCall =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt");
    Aggregate aggregate =
        new LogicalAggregate(
            cluster,
            cluster.traitSetOf(Convention.NONE),
            List.of(),
            scan,
            ImmutableBitSet.of(0),
            null,
            List.of(countCall));
    when(mq.getRowCount(aggregate)).thenReturn(1000d);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(2));
    lenient().when(relDataType.getFieldCount()).thenReturn(2);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.AGGREGATION, aggregate, action));
    assertEquals(2812.5, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_aggregate_pushdown_with_two_aggCall() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(relDataType.getFieldCount()).thenReturn(10);
    lenient().when(cluster.getTypeFactory()).thenReturn(typeFactory);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);
    AggPushDownAction action =
        new AggPushDownAction(Pair.of(List.of(), null), null, List.of()) {
          @Override
          public void apply(OpenSearchRequestBuilder requestBuilder) {}
        };
    AggregateCall countCall =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt");
    AggregateCall sumCall =
        AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(1),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "sum");
    Aggregate aggregate =
        new LogicalAggregate(
            cluster,
            cluster.traitSetOf(Convention.NONE),
            List.of(),
            scan,
            ImmutableBitSet.of(0),
            null,
            List.of(countCall, sumCall));
    when(mq.getRowCount(aggregate)).thenReturn(1000d);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(3));
    lenient().when(relDataType.getFieldCount()).thenReturn(3);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.AGGREGATION, aggregate, action));
    assertEquals(
        3836.2500429153442, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_aggregate_pushdown_with_one_aggCall_with_script() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(relDataType.getFieldCount()).thenReturn(10);
    lenient().when(cluster.getTypeFactory()).thenReturn(typeFactory);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);
    AggPushDownAction action =
        new AggPushDownAction(Pair.of(List.of(), null), null, List.of()) {
          @Override
          public void apply(OpenSearchRequestBuilder requestBuilder) {}

          @Override
          public long getScriptCount() {
            return 1;
          }
        };
    AggregateCall countCall =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt");
    Aggregate aggregate =
        new LogicalAggregate(
            cluster,
            cluster.traitSetOf(Convention.NONE),
            List.of(),
            scan,
            ImmutableBitSet.of(0),
            null,
            List.of(countCall));
    when(mq.getRowCount(aggregate)).thenReturn(1000d);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(2));
    lenient().when(relDataType.getFieldCount()).thenReturn(2);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.AGGREGATION, aggregate, action));
    assertEquals(
        2913.7500643730164, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_project_limit_pushdown() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(table.getRowType()).thenReturn(relDataType);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    List<String> projectDigest = List.of("A");
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.PROJECT, projectDigest, (OSRequestBuilderAction) req -> {}));
    LimitDigest limitDigest = new LimitDigest(100, 0);
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.LIMIT, limitDigest, (OSRequestBuilderAction) req -> {}));
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(projectDigest.size()));
    assertEquals(
        89.10000000000001, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());

    // Reverse the push down sequence won't change the cost
    scan = new CalciteLogicalIndexScan(cluster, table, osIndex);
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.LIMIT, limitDigest, (OSRequestBuilderAction) req -> {}));
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.PROJECT, projectDigest, (OSRequestBuilderAction) req -> {}));
    assertEquals(
        89.10000000000001, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_multi_operator_pushdown_without_agg() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(table.getRowType()).thenReturn(relDataType);
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    List<String> projectDigest = List.of("A", "B");
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.PROJECT, projectDigest, (OSRequestBuilderAction) req -> {}));
    RexNode condition =
        builder.makeCall(
            SqlStdOperatorTable.EQUALS,
            builder.makeInputRef(scan, 0),
            builder.makeLiteral("Hello"));
    FilterDigest filterDigest = new FilterDigest(0, condition);
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.FILTER, filterDigest, (OSRequestBuilderAction) req -> {}));
    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.SORT, null, (OSRequestBuilderAction) req -> {}));
    LimitDigest limitDigest = new LimitDigest(100, 0);
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.LIMIT, limitDigest, (OSRequestBuilderAction) req -> {}));
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(projectDigest.size()));
    assertEquals(1528.2, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  @Test
  void test_cost_on_aggregate_pushdown_along_with_others() {
    RelDataType relDataType = mock(RelDataType.class);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(10));
    lenient().when(relDataType.getFieldCount()).thenReturn(10);
    lenient().when(cluster.getTypeFactory()).thenReturn(typeFactory);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);
    AggPushDownAction action =
        new AggPushDownAction(Pair.of(List.of(), null), null, List.of()) {
          @Override
          public void apply(OpenSearchRequestBuilder requestBuilder) {}

          @Override
          public long getScriptCount() {
            return 1;
          }
        };
    AggregateCall countCall =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt");
    Aggregate aggregate =
        new LogicalAggregate(
            cluster,
            cluster.traitSetOf(Convention.NONE),
            List.of(),
            scan,
            ImmutableBitSet.of(0),
            null,
            List.of(countCall));
    when(mq.getRowCount(aggregate)).thenReturn(1000d);
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(2));
    lenient().when(relDataType.getFieldCount()).thenReturn(2);
    lenient().when(table.getRowType()).thenReturn(relDataType);

    List<String> projectDigest1 = List.of("A", "B");
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.PROJECT, projectDigest1, (OSRequestBuilderAction) req -> {}));
    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.AGGREGATION, aggregate, action));
    List<String> projectDigest2 = List.of("COUNT");
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.PROJECT, projectDigest2, (AggregationBuilderAction) req -> {}));
    scan.getPushDownContext()
        .add(new PushDownOperation(PushDownType.SORT, null, (OSRequestBuilderAction) req -> {}));
    LimitDigest limitDigest = new LimitDigest(100, 0);
    scan.getPushDownContext()
        .add(
            new PushDownOperation(
                PushDownType.LIMIT, limitDigest, (AggregationBuilderAction) req -> {}));
    lenient().when(relDataType.getFieldList()).thenReturn(new MockFieldList(projectDigest2.size()));
    assertEquals(
        2102.8500643730163, Objects.requireNonNull(scan.computeSelfCost(planner, mq)).getRows());
  }

  private static class MockFieldList extends AbstractList<RelDataTypeField> {
    private final int size;

    public MockFieldList(int size) {
      this.size = size;
    }

    @Override
    public RelDataTypeField get(int index) {
      RelDataType type =
          typeFactory.createSqlType(index == 0 ? SqlTypeName.VARCHAR : SqlTypeName.BIGINT);
      return new RelDataTypeFieldImpl("dummy", index, type);
    }

    @Override
    public int size() {
      return size;
    }
  }
}
