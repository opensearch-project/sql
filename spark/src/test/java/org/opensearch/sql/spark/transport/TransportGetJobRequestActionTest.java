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
import org.opensearch.sql.spark.transport.model.GetJobActionRequest;
import org.opensearch.sql.spark.transport.model.GetJobActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportGetJobRequestActionTest {

  @Mock private TransportService transportService;
  @Mock private TransportGetJobRequestAction action;
  @Mock private Task task;
  @Mock private ActionListener<GetJobActionResponse> actionListener;

  @Captor private ArgumentCaptor<GetJobActionResponse> getJobActionResponseArgumentCaptor;

  @BeforeEach
  public void setUp() {
    action = new TransportGetJobRequestAction(transportService, new ActionFilters(new HashSet<>()));
  }

  @Test
  public void testDoExecuteWithSingleJob() {
    GetJobActionRequest request = new GetJobActionRequest("abcd");

    action.doExecute(task, request, actionListener);
    Mockito.verify(actionListener).onResponse(getJobActionResponseArgumentCaptor.capture());
    GetJobActionResponse getJobActionResponse = getJobActionResponseArgumentCaptor.getValue();
    Assertions.assertEquals("Job abcd details.", getJobActionResponse.getResult());
  }

  @Test
  public void testDoExecuteWithAllJobs() {
    GetJobActionRequest request = new GetJobActionRequest();
    action.doExecute(task, request, actionListener);
    Mockito.verify(actionListener).onResponse(getJobActionResponseArgumentCaptor.capture());
    GetJobActionResponse getJobActionResponse = getJobActionResponseArgumentCaptor.getValue();
    Assertions.assertEquals("All Jobs Information.", getJobActionResponse.getResult());
  }
}
