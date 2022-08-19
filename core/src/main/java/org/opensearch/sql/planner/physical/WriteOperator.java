/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.Collections;
import java.util.List;
import lombok.RequiredArgsConstructor;

/**
 * Write operator.
 */
@RequiredArgsConstructor
public abstract class WriteOperator extends PhysicalPlan {

  protected final PhysicalPlan input;

  protected final String tableName;

  protected final List<String> columns;

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitWrite(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }
}
