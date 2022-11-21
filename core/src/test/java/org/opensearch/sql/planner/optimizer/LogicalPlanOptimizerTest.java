/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.optimizer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.longValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.aggregation;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.filter;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.highlight;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.limit;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.project;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.relation;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.sort;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.TableScanBuilder;

@ExtendWith(MockitoExtension.class)
class LogicalPlanOptimizerTest {

  @Mock
  private Table table;

  @Spy
  private TableScanBuilder tableScanBuilder;

  @BeforeEach
  void setUp() {
    when(table.createScanBuilder()).thenReturn(tableScanBuilder);
  }

  /**
   * Filter - Filter --> Filter.
   */
  @Test
  void filter_merge_filter() {
    assertEquals(
        filter(
            tableScanBuilder,
            DSL.and(DSL.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(2))),
                DSL.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1))))
        ),
        optimize(
            filter(
                filter(
                    relation("schema", table),
                    DSL.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(1)))
                ),
                DSL.equal(DSL.ref("integer_value", INTEGER), DSL.literal(integerValue(2)))
            )
        )
    );
  }

  /**
   * Filter - Sort --> Sort - Filter.
   */
  @Test
  void push_filter_under_sort() {
    assertEquals(
        sort(
            filter(
                tableScanBuilder,
                DSL.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1)))
            ),
            Pair.of(Sort.SortOption.DEFAULT_ASC, DSL.ref("longV", LONG))
        ),
        optimize(
            filter(
                sort(
                    relation("schema", table),
                    Pair.of(Sort.SortOption.DEFAULT_ASC, DSL.ref("longV", LONG))
                ),
                DSL.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1)))
            )
        )
    );
  }

  /**
   * Filter - Sort --> Sort - Filter.
   */
  @Test
  void multiple_filter_should_eventually_be_merged() {
    assertEquals(
        sort(
            filter(
                tableScanBuilder,
                DSL.and(DSL.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1))),
                    DSL.less(DSL.ref("longV", INTEGER), DSL.literal(longValue(1L))))
            ),
            Pair.of(Sort.SortOption.DEFAULT_ASC, DSL.ref("longV", LONG))
        ),
        optimize(
            filter(
                sort(
                    filter(
                        relation("schema", table),
                        DSL.less(DSL.ref("longV", INTEGER), DSL.literal(longValue(1L)))
                    ),
                    Pair.of(Sort.SortOption.DEFAULT_ASC, DSL.ref("longV", LONG))
                ),
                DSL.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1)))
            )
        )
    );
  }

  @Test
  void default_table_scan_builder_should_not_push_down_anything() {
    LogicalPlan[] plans = {
        project(
            relation("schema", table),
            DSL.named("i", DSL.ref("intV", INTEGER))
        ),
        filter(
            relation("schema", table),
            DSL.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1)))
        ),
        aggregation(
            relation("schema", table),
            ImmutableList
                .of(DSL.named("AVG(intV)",
                    DSL.avg(DSL.ref("intV", INTEGER)))),
            ImmutableList.of(DSL.named("longV", DSL.ref("longV", LONG)))),
        sort(
            relation("schema", table),
            Pair.of(Sort.SortOption.DEFAULT_ASC, DSL.ref("intV", INTEGER))),
        limit(
            relation("schema", table),
            1, 1)
    };

    for (LogicalPlan plan : plans) {
      assertEquals(plan, optimize(plan));
    }
  }

  @Test
  void table_scan_builder_support_project_push_down_can_apply_its_rule() {
    when(tableScanBuilder.pushDownProject(any())).thenReturn(true);

    assertEquals(
        tableScanBuilder,
        optimize(
            project(
                relation("schema", table),
                DSL.named("i", DSL.ref("intV", INTEGER)))
        )
    );
  }

  @Test
  void table_scan_builder_support_filter_push_down_can_apply_its_rule() {
    when(tableScanBuilder.pushDownFilter(any())).thenReturn(true);

    assertEquals(
        tableScanBuilder,
        optimize(
            filter(
                relation("schema", table),
                DSL.equal(DSL.ref("intV", INTEGER), DSL.literal(integerValue(1))))
        )
    );
  }

  @Test
  void table_scan_builder_support_aggregation_push_down_can_apply_its_rule() {
    when(tableScanBuilder.pushDownAggregation(any())).thenReturn(true);

    assertEquals(
        tableScanBuilder,
        optimize(
            aggregation(
                relation("schema", table),
                ImmutableList
                    .of(DSL.named("AVG(intV)",
                        DSL.avg(DSL.ref("intV", INTEGER)))),
                ImmutableList.of(DSL.named("longV", DSL.ref("longV", LONG))))
        )
    );
  }

  @Test
  void table_scan_builder_support_sort_push_down_can_apply_its_rule() {
    when(tableScanBuilder.pushDownSort(any())).thenReturn(true);

    assertEquals(
        tableScanBuilder,
        optimize(
            sort(
                relation("schema", table),
                Pair.of(Sort.SortOption.DEFAULT_ASC, DSL.ref("intV", INTEGER)))
        )
    );
  }

  @Test
  void table_scan_builder_support_limit_push_down_can_apply_its_rule() {
    when(tableScanBuilder.pushDownLimit(any())).thenReturn(true);

    assertEquals(
        tableScanBuilder,
        optimize(
            limit(
                relation("schema", table),
                1, 1)
        )
    );
  }

  @Test
  void table_scan_builder_support_highlight_push_down_can_apply_its_rule() {
    when(tableScanBuilder.pushDownHighlight(any())).thenReturn(true);

    assertEquals(
        tableScanBuilder,
        optimize(
            highlight(
                relation("schema", table),
                DSL.named("i", DSL.ref("intV", INTEGER)),
                Collections.emptyMap())
        )
    );
  }

  @Test
  void table_not_support_scan_builder_should_not_be_impact() {
    Mockito.reset(table, tableScanBuilder);
    Table table = new Table() {
      @Override
      public Map<String, ExprType> getFieldTypes() {
        return null;
      }

      @Override
      public PhysicalPlan implement(LogicalPlan plan) {
        return null;
      }
    };

    assertEquals(
        relation("schema", table),
        optimize(relation("schema", table))
    );
  }

  private LogicalPlan optimize(LogicalPlan plan) {
    final LogicalPlanOptimizer optimizer = LogicalPlanOptimizer.create();
    final LogicalPlan optimize = optimizer.optimize(plan);
    return optimize;
  }
}
