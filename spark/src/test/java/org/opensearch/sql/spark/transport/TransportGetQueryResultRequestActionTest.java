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
import org.opensearch.sql.spark.transport.model.GetJobQueryResultActionRequest;
import org.opensearch.sql.spark.transport.model.GetJobQueryResultActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportGetQueryResultRequestActionTest {

  @Mock private TransportService transportService;
  @Mock private TransportGetQueryResultRequestAction action;
  @Mock private Task task;
  @Mock private ActionListener<GetJobQueryResultActionResponse> actionListener;

  @Captor
  private ArgumentCaptor<GetJobQueryResultActionResponse> createJobActionResponseArgumentCaptor;

  @BeforeEach
  public void setUp() {
    action =
        new TransportGetQueryResultRequestAction(
            transportService, new ActionFilters(new HashSet<>()));
  }

  @Test
  public void testDoExecuteForSingleJob() {
    GetJobQueryResultActionRequest request = new GetJobQueryResultActionRequest("jobId");
    action.doExecute(task, request, actionListener);
    Mockito.verify(actionListener).onResponse(createJobActionResponseArgumentCaptor.capture());
    GetJobQueryResultActionResponse getJobQueryResultActionResponse =
        createJobActionResponseArgumentCaptor.getValue();
    Assertions.assertEquals("job result", getJobQueryResultActionResponse.getResult());
  }
}
