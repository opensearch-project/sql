/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.executor.ExecutionEngine.Schema;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.ddl.DataDefinitionTask;
import org.opensearch.sql.executor.ExecutionEngine;

/**
 * Data definition physical plan that wraps DDL executable task.
 */
@Getter
@RequiredArgsConstructor
public class DataDefinitionOperator extends PhysicalPlan {

  private int counter = 1;

  private final DataDefinitionTask task;

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitDDL(this, context);
  }

  @Override
  public Schema schema() {
    return new Schema(Arrays.asList(
        new Schema.Column("message", "message", ExprCoreType.STRING)));
  }

  @Override
  public boolean hasNext() {
    return (counter == 1);
  }

  @Override
  public ExprValue next() {
    try {
      counter++;
      ExprValue response = task.execute();
      return response.isMissing() ? tupleValue(
          ImmutableMap.of("message", stringValue("1 row impacted"))) : response;
    } catch (Exception e) {
      throw new RuntimeException(e); // Throw execution exception?
    }
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.emptyList();
  }
}
