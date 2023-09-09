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
import org.opensearch.sql.spark.transport.model.DeleteJobActionRequest;
import org.opensearch.sql.spark.transport.model.DeleteJobActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportDeleteJobRequestActionTest {

  @Mock private TransportService transportService;
  @Mock private TransportDeleteJobRequestAction action;
  @Mock private Task task;
  @Mock private ActionListener<DeleteJobActionResponse> actionListener;

  @Captor private ArgumentCaptor<DeleteJobActionResponse> deleteJobActionResponseArgumentCaptor;

  @BeforeEach
  public void setUp() {
    action =
        new TransportDeleteJobRequestAction(transportService, new ActionFilters(new HashSet<>()));
  }

  @Test
  public void testDoExecute() {
    DeleteJobActionRequest request = new DeleteJobActionRequest("jobId");

    action.doExecute(task, request, actionListener);
    Mockito.verify(actionListener).onResponse(deleteJobActionResponseArgumentCaptor.capture());
    DeleteJobActionResponse deleteJobActionResponse =
        deleteJobActionResponseArgumentCaptor.getValue();
    Assertions.assertEquals("deleted_job", deleteJobActionResponse.getResult());
  }
}
