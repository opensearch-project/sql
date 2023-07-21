/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.executor;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * Used for testing purpose.
 */
public class DefaultExecutionEngine implements ExecutionEngine {
  @Override
  public void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    execute(plan, ExecutionContext.emptyExecutionContext(), listener);
  }

  @Override
  public void execute(
      PhysicalPlan plan, ExecutionContext context, ResponseListener<QueryResponse> listener) {
    try {
      List<ExprValue> result = new ArrayList<>();

      context.getSplit().ifPresent(plan::add);
      plan.open();

      while (plan.hasNext()) {
        result.add(plan.next());
      }
      QueryResponse response = new QueryResponse(new Schema(new ArrayList<>()), new ArrayList<>(),
          Cursor.None);
      listener.onResponse(response);
    } catch (Exception e) {
      listener.onFailure(e);
    } finally {
      plan.close();
    }
  }

  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    throw new UnsupportedOperationException();
  }
}
