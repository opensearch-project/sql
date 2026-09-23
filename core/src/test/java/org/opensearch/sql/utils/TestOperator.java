/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import java.util.List;
import lombok.Setter;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

public class TestOperator extends PhysicalPlan {
  private int field;
  @Setter private boolean throwNoCursorOnWrite = false;
  @Setter private boolean throwIoOnWrite = false;

  public TestOperator() {}

  public TestOperator(int value) {
    field = value;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof TestOperator && field == ((TestOperator) o).field;
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return null;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public ExprValue next() {
    return null;
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return null;
  }
}
