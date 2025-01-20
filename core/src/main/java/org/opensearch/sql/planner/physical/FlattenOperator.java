/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;

@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class FlattenOperator extends PhysicalPlan {

  // TODO #3030: Implement

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return null;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public ExprValue next() {
    return null;
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return List.of();
  }
}
