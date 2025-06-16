/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
import org.opensearch.sql.ast.statement.Explain;
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

  @Mock private Explain.ExplainFormat format;

  @Test
  public void execute() {
    doNothing().when(queryPlan).explain(any(), any());

    ExplainPlan explainPlan =
        new ExplainPlan(queryId, queryType, queryPlan, format, explainListener);
    explainPlan.execute();

    verify(queryPlan, times(1)).explain(explainListener, format);
  }

  @Test
  public void explainThrowException() {
    ExplainPlan explainPlan =
        new ExplainPlan(queryId, queryType, queryPlan, format, explainListener);

    UnsupportedOperationException unsupportedExplainException =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              explainPlan.explain(explainListener, format);
            });
    assertEquals("explain query can not been explained.", unsupportedExplainException.getMessage());
  }
}
