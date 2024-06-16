/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.transport;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.constants.TestConstants.EMR_JOB_ID;

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
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceImpl;
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

  @Mock private AsyncQueryExecutorServiceImpl asyncQueryExecutorService;

  @Captor
  private ArgumentCaptor<CancelAsyncQueryActionResponse> deleteJobActionResponseArgumentCaptor;

  @Captor private ArgumentCaptor<Exception> exceptionArgumentCaptor;

  @BeforeEach
  public void setUp() {
    action =
        new TransportCancelAsyncQueryRequestAction(
            transportService, new ActionFilters(new HashSet<>()), asyncQueryExecutorService);
  }

  @Test
  public void testDoExecute() {
    CancelAsyncQueryActionRequest request = new CancelAsyncQueryActionRequest(EMR_JOB_ID);
    when(asyncQueryExecutorService.cancelQuery(EMR_JOB_ID)).thenReturn(EMR_JOB_ID);
    action.doExecute(task, request, actionListener);
    Mockito.verify(actionListener).onResponse(deleteJobActionResponseArgumentCaptor.capture());
    CancelAsyncQueryActionResponse cancelAsyncQueryActionResponse =
        deleteJobActionResponseArgumentCaptor.getValue();
    Assertions.assertEquals(
        "Deleted async query with id: " + EMR_JOB_ID, cancelAsyncQueryActionResponse.getResult());
  }

  @Test
  public void testDoExecuteWithException() {
    CancelAsyncQueryActionRequest request = new CancelAsyncQueryActionRequest(EMR_JOB_ID);
    doThrow(new RuntimeException("Error")).when(asyncQueryExecutorService).cancelQuery(EMR_JOB_ID);
    action.doExecute(task, request, actionListener);
    Mockito.verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception exception = exceptionArgumentCaptor.getValue();
    Assertions.assertTrue(exception instanceof RuntimeException);
    Assertions.assertEquals("Error", exception.getMessage());
  }
}
