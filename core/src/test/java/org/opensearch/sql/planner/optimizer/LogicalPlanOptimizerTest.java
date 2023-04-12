/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.optimizer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.longValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.aggregation;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.filter;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.highlight;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.limit;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.paginate;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.project;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.relation;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.sort;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.values;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.write;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.optimizer.rule.CreatePagingTableScanBuilder;
import org.opensearch.sql.planner.optimizer.rule.read.CreateTableScanBuilder;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.read.TableScanBuilder;
import org.opensearch.sql.storage.write.TableWriteBuilder;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class LogicalPlanOptimizerTest {

  @Mock
  private Table table;

  @Spy
  private TableScanBuilder tableScanBuilder;

  @Spy
  private TableScanBuilder pagedTableScanBuilder;

  @BeforeEach
  void setUp() {
    lenient().when(table.createScanBuilder()).thenReturn(tableScanBuilder);
    lenient().when(table.createPagedScanBuilder(anyInt())).thenReturn(pagedTableScanBuilder);
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
                DSL.literal("*"),
                Collections.emptyMap())
        )
    );
  }

  @Test
  void table_not_support_scan_builder_should_not_be_impact() {
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

  @Test
  void table_support_write_builder_should_be_replaced() {
    TableWriteBuilder writeBuilder = Mockito.mock(TableWriteBuilder.class);
    when(table.createWriteBuilder(any())).thenReturn(writeBuilder);

    assertEquals(
        writeBuilder,
        optimize(write(values(), table, Collections.emptyList()))
    );
  }

  @Test
  void table_not_support_write_builder_should_report_error() {
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

    assertThrows(UnsupportedOperationException.class,
        () -> table.createWriteBuilder(null));
  }

  @Test
  void paged_table_scan_builder_support_project_push_down_can_apply_its_rule() {

    var relation = Mockito.spy(new LogicalRelation("schema", table));

    assertEquals(
        paginate(project(pagedTableScanBuilder), 4),
        LogicalPlanOptimizer.create().optimize(paginate(project(relation), 4)));
  }

  @Test
  void push_page_size() {
    var relation = new LogicalRelation("schema", table);
    var paginate = new LogicalPaginate(42, List.of(project(relation)));
    assertNull(relation.getPageSize());
    LogicalPlanOptimizer.create().optimize(paginate);
    assertEquals(42, relation.getPageSize());
  }

  @Test
  void push_page_size_noop_if_no_relation() {
    var paginate = new LogicalPaginate(42, List.of(project(values())));
    assertEquals(paginate, LogicalPlanOptimizer.create().optimize(paginate));
  }

  @Test
  void pagination_optimizer_simple_query() {
    var projectPlan = project(relation("schema", table), DSL.named(DSL.ref("intV", INTEGER)));

    var optimizer = new LogicalPlanOptimizer(
        List.of(new CreateTableScanBuilder(), new CreatePagingTableScanBuilder()));

    {
      optimizer.optimize(projectPlan);
      verify(table).createScanBuilder();
      verify(table, never()).createPagedScanBuilder(anyInt());
      // Assert that createPagedTableScan was not called
      // Assert that createTableScan was called
    }
  }

  @Test
  void pagination_optimizer_paged_query() {
    var relation = new LogicalRelation("schema", table);
    relation.setPageSize(4);
    var projectPlan = project(relation, DSL.named(DSL.ref("intV", INTEGER)));
    var pagedPlan = new LogicalPaginate(10, List.of(projectPlan));

    var optimizer = new LogicalPlanOptimizer(
        List.of(new CreateTableScanBuilder(), new CreatePagingTableScanBuilder()));
    var optimized = optimizer.optimize(pagedPlan);
    verify(table).createPagedScanBuilder(anyInt());
  }

  @Test
  void push_page_size_noop_if_no_sub_plans() {
    var paginate = new LogicalPaginate(42, List.of());
    assertEquals(paginate,
        LogicalPlanOptimizer.create().optimize(paginate));
  }

  @Test
  void table_scan_builder_support_offset_push_down_can_apply_its_rule() {
    when(table.createPagedScanBuilder(anyInt())).thenReturn(pagedTableScanBuilder);

    var relation = new LogicalRelation("schema", table);
    relation.setPageSize(4);
    var optimized = LogicalPlanOptimizer.create()
        .optimize(new LogicalPaginate(42, List.of(project(relation))));
    // `optimized` structure: LogicalPaginate -> LogicalProject -> TableScanBuilder
    // LogicalRelation replaced by a TableScanBuilder instance
    assertEquals(paginate(project(pagedTableScanBuilder), 42), optimized);
  }

  private LogicalPlan optimize(LogicalPlan plan) {
    final LogicalPlanOptimizer optimizer = LogicalPlanOptimizer.create();
    final LogicalPlan optimize = optimizer.optimize(plan);
    return optimize;
  }
}
