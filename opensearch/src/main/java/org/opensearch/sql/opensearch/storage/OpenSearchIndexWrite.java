/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.WriteOperator;

/**
 * OpenSearch index write operator.
 */
public class OpenSearchIndexWrite extends WriteOperator {

  public OpenSearchIndexWrite(PhysicalPlan input, String tableName, List<String> columns) {
    super(input, tableName, columns);
  }

  @Override
  public ExecutionEngine.Schema schema() {
    return new ExecutionEngine.Schema(Arrays.asList(
        new ExecutionEngine.Schema.Column("message", "message", ExprCoreType.STRING)));
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public ExprValue next() {
    int count = 0;
    while (input.hasNext()) {
      count++;
      input.next();
    }
    return tupleValue(
        ImmutableMap.of("message", stringValue(count + " row(s) impacted")));
  }
}
