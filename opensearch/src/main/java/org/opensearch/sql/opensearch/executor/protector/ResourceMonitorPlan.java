/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.protector;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.planner.SerializablePlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

/** A PhysicalPlan which will run the delegate plan in resource protection manner. */
@ToString
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class ResourceMonitorPlan extends PhysicalPlan implements SerializablePlan {

  /** How many method calls to delegate's next() to perform resource check once. */
  public static final long NUMBER_OF_NEXT_CALL_TO_CHECK = 1000;

  /** Delegated PhysicalPlan. */
  private final PhysicalPlan delegate;

  /** ResourceMonitor. */
  @ToString.Exclude private final ResourceMonitor monitor;

  /** Count how many calls to delegate's next() already. */
  @EqualsAndHashCode.Exclude private long nextCallCount = 0L;

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return delegate.accept(visitor, context);
  }

  @Override
  public void open() {
    if (!this.monitor.isHealthy()) {
      throw new IllegalStateException("insufficient resources to run the query, quit.");
    }
    delegate.open();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return delegate.getChild();
  }

  @Override
  public boolean hasNext() {
    return delegate.hasNext();
  }

  @Override
  public ExprValue next() {
    boolean shouldCheck = (++nextCallCount % NUMBER_OF_NEXT_CALL_TO_CHECK == 0);
    if (shouldCheck && !this.monitor.isHealthy()) {
      throw new IllegalStateException("insufficient resources to load next row, quit.");
    }
    return delegate.next();
  }

  @Override
  public SerializablePlan getPlanForSerialization() {
    return (SerializablePlan) delegate;
  }

  /**
   * Those two methods should never be called. They called if a plan upper in the tree missed to
   * call {@link #getPlanForSerialization}.
   */
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    throw new UnsupportedOperationException();
  }
}
