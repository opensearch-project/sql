/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner;

import java.util.List;

/**
 * The definition of Plan Node.
 */
public interface PlanNode<T extends PlanNode> {

  /**
   * Return the child nodes.
   *
   * @return child nodes.
   */
  List<T> getChild();
}
