/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.ast.physical;

import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

public abstract class OpenSearchPhysicalPlanNodeVisitor<R, C> extends PhysicalPlanNodeVisitor<R, C> {
  public R visitNested(NestedOperator node, C context) {
    return visitNode(node, context);
}
}
