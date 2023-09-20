/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.transport;

import java.util.HashSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.spark.transport.model.CancelAsyncQueryActionRequest;
import org.opensearch.sql.spark.transport.model.CancelAsyncQueryActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportCancelAsyncQueryRequestActionTest {

  @Mock private TransportService transportService;
  @Mock private TransportCancelAsyncQueryRequestAction action;
  @Mock private Task task;
  @Mock private ActionListener<CancelAsyncQueryActionResponse> actionListener;

  @Captor
  private ArgumentCaptor<CancelAsyncQueryActionResponse> deleteJobActionResponseArgumentCaptor;

  @BeforeEach
  public void setUp() {
    action =
        new TransportCancelAsyncQueryRequestAction(
            transportService, new ActionFilters(new HashSet<>()));
  }

  @Test
  public void testDoExecute() {
    CancelAsyncQueryActionRequest request = new CancelAsyncQueryActionRequest("jobId");

    action.doExecute(task, request, actionListener);
    Mockito.verify(actionListener).onResponse(deleteJobActionResponseArgumentCaptor.capture());
    CancelAsyncQueryActionResponse cancelAsyncQueryActionResponse =
        deleteJobActionResponseArgumentCaptor.getValue();
    Assertions.assertEquals("deleted_job", cancelAsyncQueryActionResponse.getResult());
  }
}
