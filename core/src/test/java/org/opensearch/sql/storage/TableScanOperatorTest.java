/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

class TableScanOperatorTest {

  private final TableScanOperator tableScan =
      new TableScanOperator() {
        @Override
        public String explain() {
          return "explain";
        }

        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public ExprValue next() {
          return null;
        }
      };

  @Test
  public void accept() {
    Boolean isVisited =
        tableScan.accept(
            new PhysicalPlanNodeVisitor<Boolean, Object>() {
              @Override
              protected Boolean visitNode(PhysicalPlan node, Object context) {
                return (node instanceof TableScanOperator);
              }

              @Override
              public Boolean visitTableScan(TableScanOperator node, Object context) {
                return super.visitTableScan(node, context);
              }
            },
            null);

    assertTrue(isVisited);
  }

  @Test
  public void getChild() {
    assertTrue(tableScan.getChild().isEmpty());
  }
}
