/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.optimizer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.longValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.aggregation;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.filter;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.highlight;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.limit;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.nested;
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
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.expression.OpenSearchDSL;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.read.TableScanBuilder;
import org.opensearch.sql.storage.write.TableWriteBuilder;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class LogicalPlanOptimizerTest {

  @Mock private Table table;

  @Spy private TableScanBuilder tableScanBuilder;

  @BeforeEach
  void setUp() {
    lenient().when(table.createScanBuilder()).thenReturn(tableScanBuilder);
  }

  @Test
  void table_scan_builder_support_nested_push_down_can_apply_its_rule() {
    when(tableScanBuilder.pushDownNested(any())).thenReturn(true);

    assertEquals(
        tableScanBuilder,
        optimize(
            nested(
                relation("schema", table),
                List.of(Map.of("field", new ReferenceExpression("message.info", STRING))),
                List.of(
                    new NamedExpression(
                        "message.info", OpenSearchDSL.nested(OpenSearchDSL.ref("message.info", STRING)), null)))));
  }

  private LogicalPlan optimize(LogicalPlan plan) {
    final LogicalPlanOptimizer optimizer = LogicalPlanOptimizer.create();
    return optimizer.optimize(plan);
  }
}
