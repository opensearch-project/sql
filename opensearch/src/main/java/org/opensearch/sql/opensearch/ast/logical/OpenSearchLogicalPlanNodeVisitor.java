/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.ast.logical;

import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;

public abstract class OpenSearchLogicalPlanNodeVisitor<R, C> extends LogicalPlanNodeVisitor<R, C> {

  public R visitHighlight(LogicalHighlight plan, C context) {
    return visitNode(plan, context);
  }

  public R visitNested(LogicalNested plan, C context) {
    return visitNode(plan, context);
  }
}
