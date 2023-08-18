/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.logical;

import java.util.Map;
import org.json.JSONPropertyIgnore;
import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.physical.PhysicalOperator;

/** Logical operator in logical plan tree. */
public interface LogicalOperator extends PlanNode {

  /**
   * If current operator is no operation. It depends on specific internal state of operator
   *
   * <p>Ignore this field in explanation because all explainable operator are NOT no-op.
   *
   * @return true if NoOp
   */
  @JSONPropertyIgnore
  default boolean isNoOp() {
    return false;
  }

  /**
   * Map logical operator to physical operators (possibly 1 to N mapping)
   *
   * <p>Note that generic type on PhysicalOperator[] would enforce all impl convert array to generic
   * type array because generic type array is unable to be created directly.
   *
   * @param optimalOps optimal physical operators estimated so far
   * @return list of physical operator
   */
  <T> PhysicalOperator[] toPhysical(Map<LogicalOperator, PhysicalOperator<T>> optimalOps);
}
