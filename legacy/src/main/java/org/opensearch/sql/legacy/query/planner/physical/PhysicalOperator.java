/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.physical;

import java.util.Iterator;
import org.opensearch.sql.legacy.query.planner.core.ExecuteParams;
import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.physical.estimation.Cost;

/** Physical operator */
public interface PhysicalOperator<T> extends PlanNode, Iterator<Row<T>>, AutoCloseable {

  /**
   * Estimate the cost of current physical operator
   *
   * @return cost
   */
  Cost estimate();

  /**
   * Initialize operator.
   *
   * @param params exuecution parameters needed
   */
  default void open(ExecuteParams params) throws Exception {
    for (PlanNode node : children()) {
      ((PhysicalOperator) node).open(params);
    }
  }

  /**
   * Close resources related to the operator.
   *
   * @throws Exception potential exception raised
   */
  @Override
  default void close() {
    for (PlanNode node : children()) {
      ((PhysicalOperator) node).close();
    }
  }
}
