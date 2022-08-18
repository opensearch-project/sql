/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl.view;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.planner.logical.LogicalPlan;

/**
 * View definition.
 */
@Getter
@RequiredArgsConstructor
@ToString
public class ViewDefinition {

  /**
   * View type.
   */
  public enum ViewType {
    MATERIALIZED_VIEW,
    SKIPPING_INDEX,
    COVERING_INDEX
  }

  private final String viewName;

  private final ViewType viewType;

  private UnresolvedPlan query;

}
