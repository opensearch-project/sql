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
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.spark.jobs.JobExecutorServiceImpl;
import org.opensearch.sql.spark.jobs.exceptions.JobNotFoundException;
import org.opensearch.sql.spark.jobs.model.JobExecutionResponse;
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
  @Mock private JobExecutorServiceImpl jobExecutorService;

  @Captor
  private ArgumentCaptor<GetJobQueryResultActionResponse> createJobActionResponseArgumentCaptor;

  @Captor private ArgumentCaptor<Exception> exceptionArgumentCaptor;

  @BeforeEach
  public void setUp() {
    action =
        new TransportGetQueryResultRequestAction(
            transportService, new ActionFilters(new HashSet<>()), jobExecutorService);
  }

  @Test
  public void testDoExecute() {
    GetJobQueryResultActionRequest request = new GetJobQueryResultActionRequest("jobId");
    JobExecutionResponse jobExecutionResponse = new JobExecutionResponse("IN_PROGRESS", null, null);
    when(jobExecutorService.getJobResults("jobId")).thenReturn(jobExecutionResponse);
    action.doExecute(task, request, actionListener);
    verify(actionListener).onResponse(createJobActionResponseArgumentCaptor.capture());
    GetJobQueryResultActionResponse getJobQueryResultActionResponse =
        createJobActionResponseArgumentCaptor.getValue();
    Assertions.assertEquals(
        "{\"status\":\"IN_PROGRESS\"}", getJobQueryResultActionResponse.getResult());
  }

  @Test
  public void testDoExecuteWithSuccessResponse() {
    GetJobQueryResultActionRequest request = new GetJobQueryResultActionRequest("jobId");
    ExecutionEngine.Schema schema =
        new ExecutionEngine.Schema(
            ImmutableList.of(
                new ExecutionEngine.Schema.Column("name", "name", STRING),
                new ExecutionEngine.Schema.Column("age", "age", INTEGER)));
    JobExecutionResponse jobExecutionResponse =
        new JobExecutionResponse(
            "SUCCESS",
            schema,
            Arrays.asList(
                tupleValue(ImmutableMap.of("name", "John", "age", 20)),
                tupleValue(ImmutableMap.of("name", "Smith", "age", 30))));
    when(jobExecutorService.getJobResults("jobId")).thenReturn(jobExecutionResponse);
    action.doExecute(task, request, actionListener);
    verify(actionListener).onResponse(createJobActionResponseArgumentCaptor.capture());
    GetJobQueryResultActionResponse getJobQueryResultActionResponse =
        createJobActionResponseArgumentCaptor.getValue();
    Assertions.assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"integer\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"John\",\n"
            + "      20\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Smith\",\n"
            + "      30\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 2,\n"
            + "  \"size\": 2\n"
            + "}",
        getJobQueryResultActionResponse.getResult());
  }

  @Test
  public void testDoExecuteWithException() {
    GetJobQueryResultActionRequest request = new GetJobQueryResultActionRequest("123");
    doThrow(new JobNotFoundException("JobId 123 not found"))
        .when(jobExecutorService)
        .getJobResults("123");
    action.doExecute(task, request, actionListener);
    verify(jobExecutorService, times(1)).getJobResults("123");
    verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception exception = exceptionArgumentCaptor.getValue();
    Assertions.assertTrue(exception instanceof RuntimeException);
    Assertions.assertEquals("JobId 123 not found", exception.getMessage());
  }
}
