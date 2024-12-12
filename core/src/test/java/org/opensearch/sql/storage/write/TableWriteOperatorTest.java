/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

@ExtendWith(MockitoExtension.class)
class TableWriteOperatorTest {

  @Mock private PhysicalPlan child;

  private TableWriteOperator tableWrite;

  @BeforeEach
  void setUp() {
    tableWrite =
        new TableWriteOperator(child) {
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
  }

  @Test
  void testAccept() {
    Boolean isVisited =
        tableWrite.accept(
            new PhysicalPlanNodeVisitor<>() {
              @Override
              protected Boolean visitNode(PhysicalPlan node, Object context) {
                return (node instanceof TableWriteOperator);
              }

              @Override
              public Boolean visitTableWrite(TableWriteOperator node, Object context) {
                return super.visitTableWrite(node, context);
              }
            },
            null);

    assertTrue(isVisited);
  }

  @Test
  void testGetChild() {
    assertEquals(Collections.singletonList(child), tableWrite.getChild());
  }
}
