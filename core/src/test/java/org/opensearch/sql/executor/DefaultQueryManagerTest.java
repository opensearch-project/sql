/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.execution.QueryExecution;

@ExtendWith(MockitoExtension.class)
class DefaultQueryManagerTest {

  @Mock
  private QueryExecution queryExecution;

  @Mock
  private ResponseListener<?> listener;

  @Test
  public void submitQuery() {
    new DefaultQueryManager().submitQuery(queryExecution, listener);

    verify(queryExecution, times(1)).registerListener(any());
    verify(queryExecution, times(1)).start();
  }

}
