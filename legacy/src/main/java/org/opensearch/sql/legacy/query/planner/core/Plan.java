/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.core;

import org.opensearch.sql.legacy.query.planner.core.PlanNode.Visitor;

/** Query plan */
public interface Plan {

  /**
   * Explain current query plan by visitor
   *
   * @param explanation visitor to explain the plan
   */
  void traverse(Visitor explanation);

  /** Optimize current query plan to get the optimal one */
  void optimize();
}
