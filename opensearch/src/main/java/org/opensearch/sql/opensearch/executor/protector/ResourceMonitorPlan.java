/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.protector;

import static org.opensearch.common.settings.Settings.EMPTY;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.monitor.ResourceStatus;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
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

  /**
   * Helper method to get the default memory limit string from the Setting constant.
   *
   * @return Default memory limit string (e.g., "85%")
   */
  private static String getDefaultMemoryLimit() {
    return OpenSearchSettings.QUERY_MEMORY_LIMIT_SETTING.getDefault(EMPTY).toString();
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return delegate.accept(visitor, context);
  }

  @Override
  public void open() {
    ResourceStatus status = this.monitor.getStatus();
    if (!status.isHealthy()) {
      throw new IllegalStateException(
          String.format(
              "Insufficient resources to start query: %s. "
                  + "To increase the limit, adjust the '%s' setting (default: %s).",
              status.getFormattedDescription(),
              Settings.Key.QUERY_MEMORY_LIMIT.getKeyValue(),
              getDefaultMemoryLimit()));
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
    if (shouldCheck) {
      ResourceStatus status = this.monitor.getStatus();
      if (!status.isHealthy()) {
        throw new IllegalStateException(
            String.format(
                "Insufficient resources to continue processing query: %s. "
                    + "Rows processed: %d. "
                    + "To increase the limit, adjust the '%s' setting (default: %s).",
                status.getFormattedDescription(),
                nextCallCount,
                Settings.Key.QUERY_MEMORY_LIMIT.getKeyValue(),
                getDefaultMemoryLimit()));
      }
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
