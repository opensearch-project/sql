/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.transport;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.opensearch.sql.spark.jobs.JobExecutorServiceImpl;
import org.opensearch.sql.spark.rest.model.CreateJobRequest;
import org.opensearch.sql.spark.rest.model.CreateJobResponse;
import org.opensearch.sql.spark.transport.model.CreateJobActionRequest;
import org.opensearch.sql.spark.transport.model.CreateJobActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportCreateJobRequestActionTest {

  @Mock private TransportService transportService;
  @Mock private TransportCreateJobRequestAction action;
  @Mock private JobExecutorServiceImpl jobExecutorService;
  @Mock private Task task;
  @Mock private ActionListener<CreateJobActionResponse> actionListener;

  @Captor private ArgumentCaptor<CreateJobActionResponse> createJobActionResponseArgumentCaptor;
  @Captor private ArgumentCaptor<Exception> exceptionArgumentCaptor;

  @BeforeEach
  public void setUp() {
    action =
        new TransportCreateJobRequestAction(
            transportService, new ActionFilters(new HashSet<>()), jobExecutorService);
  }

  @Test
  public void testDoExecute() {
    CreateJobRequest createJobRequest = new CreateJobRequest("source = my_glue.default.alb_logs");
    CreateJobActionRequest request = new CreateJobActionRequest(createJobRequest);
    when(jobExecutorService.createJob(createJobRequest)).thenReturn(new CreateJobResponse("123"));
    action.doExecute(task, request, actionListener);
    Mockito.verify(actionListener).onResponse(createJobActionResponseArgumentCaptor.capture());
    CreateJobActionResponse createJobActionResponse =
        createJobActionResponseArgumentCaptor.getValue();
    Assertions.assertEquals(
        "{\n" + "  \"jobId\": \"123\"\n" + "}", createJobActionResponse.getResult());
  }

  @Test
  public void testDoExecuteWithException() {
    CreateJobRequest createJobRequest = new CreateJobRequest("source = my_glue.default.alb_logs");
    CreateJobActionRequest request = new CreateJobActionRequest(createJobRequest);
    doThrow(new RuntimeException("Error")).when(jobExecutorService).createJob(createJobRequest);
    action.doExecute(task, request, actionListener);
    verify(jobExecutorService, times(1)).createJob(createJobRequest);
    Mockito.verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception exception = exceptionArgumentCaptor.getValue();
    Assertions.assertTrue(exception instanceof RuntimeException);
    Assertions.assertEquals("Error", exception.getMessage());
  }
}
