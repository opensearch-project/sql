/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import static org.junit.Assert.assertThrows;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.Client;
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.session.SessionModel;

@ExtendWith(MockitoExtension.class)
class SessionStateStoreTest {
  @Mock(answer = RETURNS_DEEP_STUBS)
  private Client client;

  @Mock private IndexResponse indexResponse;

  @Test
  public void createWithException() {
    when(client.index(any()).actionGet()).thenReturn(indexResponse);
    doReturn(DocWriteResponse.Result.NOT_FOUND).when(indexResponse).getResult();
    SessionModel sessionModel =
        SessionModel.initInteractiveSession(
            "appId", "jobId", SessionId.newSessionId(), "datasource");
    SessionStateStore sessionStateStore = new SessionStateStore("indexName", client);

    assertThrows(RuntimeException.class, () -> sessionStateStore.create(sessionModel));
  }
}
