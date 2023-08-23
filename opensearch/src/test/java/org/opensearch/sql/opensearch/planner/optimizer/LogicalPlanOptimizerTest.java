/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.optimizer;

import static org.mockito.Mockito.lenient;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.read.TableScanBuilder;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class LogicalPlanOptimizerTest {

  @Mock private Table table;

  @Spy private TableScanBuilder tableScanBuilder;

  @BeforeEach
  void setUp() {
    lenient().when(table.createScanBuilder()).thenReturn(tableScanBuilder);
  }

  //  @Test
  //  void table_scan_builder_support_nested_push_down_can_apply_its_rule() {
  //    when(tableScanBuilder.pushDownNested(any())).thenReturn(true);
  //
  //    assertEquals(
  //        tableScanBuilder,
  //        optimize(
  //            nested(
  //                relation("schema", table),
  //                List.of(Map.of("field", new ReferenceExpression("message.info", STRING))),
  //                List.of(
  //                    new NamedExpression(
  //                        "message.info", OpenSearchDSL.nested(OpenSearchDSL.ref("message.info",
  // STRING)), null)))));
  //  }

  private LogicalPlan optimize(LogicalPlan plan) {
    final LogicalPlanOptimizer optimizer = LogicalPlanOptimizer.create();
    return optimizer.optimize(plan);
  }
}
