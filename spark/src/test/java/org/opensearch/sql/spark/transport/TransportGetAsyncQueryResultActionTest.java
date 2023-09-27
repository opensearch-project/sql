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
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceImpl;
import org.opensearch.sql.spark.asyncquery.exceptions.AsyncQueryNotFoundException;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.transport.model.GetAsyncQueryResultActionRequest;
import org.opensearch.sql.spark.transport.model.GetAsyncQueryResultActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportGetAsyncQueryResultActionTest {

  @Mock private TransportService transportService;
  @Mock private TransportGetAsyncQueryResultAction action;
  @Mock private Task task;
  @Mock private ActionListener<GetAsyncQueryResultActionResponse> actionListener;
  @Mock private AsyncQueryExecutorServiceImpl jobExecutorService;

  @Captor
  private ArgumentCaptor<GetAsyncQueryResultActionResponse> createJobActionResponseArgumentCaptor;

  @Captor private ArgumentCaptor<Exception> exceptionArgumentCaptor;

  @BeforeEach
  public void setUp() {
    action =
        new TransportGetAsyncQueryResultAction(
            transportService, new ActionFilters(new HashSet<>()), jobExecutorService);
  }

  @Test
  public void testDoExecute() {
    GetAsyncQueryResultActionRequest request = new GetAsyncQueryResultActionRequest("jobId");
    AsyncQueryExecutionResponse asyncQueryExecutionResponse =
        new AsyncQueryExecutionResponse("IN_PROGRESS", null, null);
    when(jobExecutorService.getAsyncQueryResults("jobId")).thenReturn(asyncQueryExecutionResponse);
    action.doExecute(task, request, actionListener);
    verify(actionListener).onResponse(createJobActionResponseArgumentCaptor.capture());
    GetAsyncQueryResultActionResponse getAsyncQueryResultActionResponse =
        createJobActionResponseArgumentCaptor.getValue();
    Assertions.assertEquals(
        "{\n" + "  \"status\": \"IN_PROGRESS\"\n" + "}",
        getAsyncQueryResultActionResponse.getResult());
  }

  @Test
  public void testDoExecuteWithSuccessResponse() {
    GetAsyncQueryResultActionRequest request = new GetAsyncQueryResultActionRequest("jobId");
    ExecutionEngine.Schema schema =
        new ExecutionEngine.Schema(
            ImmutableList.of(
                new ExecutionEngine.Schema.Column("name", "name", STRING),
                new ExecutionEngine.Schema.Column("age", "age", INTEGER)));
    AsyncQueryExecutionResponse asyncQueryExecutionResponse =
        new AsyncQueryExecutionResponse(
            "SUCCESS",
            schema,
            Arrays.asList(
                tupleValue(ImmutableMap.of("name", "John", "age", 20)),
                tupleValue(ImmutableMap.of("name", "Smith", "age", 30))));
    when(jobExecutorService.getAsyncQueryResults("jobId")).thenReturn(asyncQueryExecutionResponse);
    action.doExecute(task, request, actionListener);
    verify(actionListener).onResponse(createJobActionResponseArgumentCaptor.capture());
    GetAsyncQueryResultActionResponse getAsyncQueryResultActionResponse =
        createJobActionResponseArgumentCaptor.getValue();
    Assertions.assertEquals(
        "{\n"
            + "  \"status\": \"SUCCESS\",\n"
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
        getAsyncQueryResultActionResponse.getResult());
  }

  @Test
  public void testDoExecuteWithException() {
    GetAsyncQueryResultActionRequest request = new GetAsyncQueryResultActionRequest("123");
    doThrow(new AsyncQueryNotFoundException("JobId 123 not found"))
        .when(jobExecutorService)
        .getAsyncQueryResults("123");
    action.doExecute(task, request, actionListener);
    verify(jobExecutorService, times(1)).getAsyncQueryResults("123");
    verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception exception = exceptionArgumentCaptor.getValue();
    Assertions.assertTrue(exception instanceof RuntimeException);
    Assertions.assertEquals("JobId 123 not found", exception.getMessage());
  }
}
