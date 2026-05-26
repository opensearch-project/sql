/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryType;

@ExtendWith(MockitoExtension.class)
public class ExplainPlanTest {
  @Mock private QueryId queryId;

  @Mock private QueryType queryType;

  @Mock private QueryPlan queryPlan;

  @Mock private ResponseListener<ExecutionEngine.ExplainResponse> explainListener;

  @Mock private ExplainMode mode;

  @Test
  public void execute() {
    doNothing().when(queryPlan).explain(any(), any());

    ExplainPlan explainPlan = new ExplainPlan(queryId, queryType, queryPlan, mode, explainListener);
    explainPlan.execute();

    verify(queryPlan, times(1)).explain(explainListener, mode);
  }

  @Test
  public void explainThrowException() {
    ExplainPlan explainPlan = new ExplainPlan(queryId, queryType, queryPlan, mode, explainListener);

    UnsupportedOperationException unsupportedExplainException =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              explainPlan.explain(explainListener, mode);
            });
    assertEquals("explain query can not been explained.", unsupportedExplainException.getMessage());
  }
}
