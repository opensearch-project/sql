/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.protector;

import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

/** Execution Plan Protector. */
public abstract class ExecutionProtector extends PhysicalPlanNodeVisitor<PhysicalPlan, Object> {

  /** Decorated the PhysicalPlan to run in resource sensitive mode. */
  public abstract PhysicalPlan protect(PhysicalPlan physicalPlan);
}
