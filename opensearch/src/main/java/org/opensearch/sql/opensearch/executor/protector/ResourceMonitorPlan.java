/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.opensearch.executor.protector;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

/**
 * A PhysicalPlan which will run the delegate plan in resource protection manner.
 */
@ToString
@RequiredArgsConstructor
@EqualsAndHashCode
public class ResourceMonitorPlan extends PhysicalPlan {

  /**
   * How many method calls to delegate's next() to perform resource check once.
   */
  public static final long NUMBER_OF_NEXT_CALL_TO_CHECK = 1000;

  /**
   * Delegated PhysicalPlan.
   */
  private final PhysicalPlan delegate;

  /**
   * ResourceMonitor.
   */
  @ToString.Exclude
  private final ResourceMonitor monitor;

  /**
   * Count how many calls to delegate's next() already.
   */
  @EqualsAndHashCode.Exclude
  private long nextCallCount = 0L;


  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return delegate.accept(visitor, context);
  }

  @Override
  public void open() {
    if (!this.monitor.isHealthy()) {
      throw new IllegalStateException("resource is not enough to run the query, quit.");
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
      throw new IllegalStateException("resource is not enough to load next row, quit.");
    }
    return delegate.next();
  }
}
