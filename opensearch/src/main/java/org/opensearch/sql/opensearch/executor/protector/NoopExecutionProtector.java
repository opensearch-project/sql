/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.protector;

import org.opensearch.sql.planner.physical.PhysicalPlan;

/** No operation execution protector. */
public class NoopExecutionProtector extends ExecutionProtector {

  @Override
  public PhysicalPlan protect(PhysicalPlan physicalPlan) {
    return physicalPlan;
  }
}
