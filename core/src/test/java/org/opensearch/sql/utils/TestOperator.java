/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.utils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import lombok.Setter;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.NoCursorException;
import org.opensearch.sql.planner.SerializablePlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

public class TestOperator extends PhysicalPlan implements SerializablePlan {
  private int field;
  @Setter
  private boolean throwNoCursorOnWrite = false;
  @Setter
  private boolean throwIoOnWrite = false;

  public TestOperator() {
  }

  public TestOperator(int value) {
    field = value;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    field = in.readInt();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    if (throwNoCursorOnWrite) {
      throw new NoCursorException();
    }
    if (throwIoOnWrite) {
      throw new IOException();
    }
    out.writeInt(field);
  }

  @Override
  public boolean equals(Object o) {
    return field == ((TestOperator) o).field;
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
