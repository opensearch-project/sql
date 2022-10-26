/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.executor.execution.AbstractPlan;

@ExtendWith(MockitoExtension.class)
class DefaultQueryManagerTest {

  @Mock
  private AbstractPlan queryExecution;

  @Mock
  private QueryId queryId;

  @Test
  public void submitQuery() {
    when(queryExecution.getQueryId()).thenReturn(queryId);

    QueryId actualQueryId = new DefaultQueryManager().submitQuery(queryExecution);

    assertEquals(queryId, actualQueryId);
    verify(queryExecution, times(1)).execute();
  }
}
