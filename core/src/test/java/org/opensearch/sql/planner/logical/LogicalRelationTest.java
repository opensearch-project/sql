/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.storage.Table;

@ExtendWith(MockitoExtension.class)
class LogicalRelationTest {

  @Mock Table table;

  @Test
  public void logicalRelationHasNoInput() {
    LogicalPlan relation = LogicalPlanDSL.relation("index", table);
    assertEquals(0, relation.getChild().size());
  }

  @Test
  public void logicalRelationWithDataSourceHasNoInput() {
    LogicalPlan relation = LogicalPlanDSL.relation("prometheus.index", table);
    assertEquals(0, relation.getChild().size());
  }
}
