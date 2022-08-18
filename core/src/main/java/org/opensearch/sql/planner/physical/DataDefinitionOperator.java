/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.Collections;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.ddl.DataDefinitionTask;

/**
 * Data definition physical plan that wraps DDL executable task.
 */
@RequiredArgsConstructor
public class DataDefinitionOperator extends PhysicalPlan {

  private int counter = 1;

  private final DataDefinitionTask task;

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return null;
  }

  @Override
  public boolean hasNext() {
    return (counter == 1);
  }

  @Override
  public ExprValue next() {
    try {
      counter++;
      task.execute();
      return ExprValueUtils.stringValue("1 row impacted");
    } catch (Exception e) {
      throw new RuntimeException(e); // Throw execution exception?
    }
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.emptyList();
  }
}
