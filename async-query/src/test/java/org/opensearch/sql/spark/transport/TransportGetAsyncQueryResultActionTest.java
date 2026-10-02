/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.job.QueryJobId;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceImpl;
import org.opensearch.sql.spark.asyncquery.exceptions.AsyncQueryNotFoundException;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.NullAsyncQueryRequestContext;
import org.opensearch.sql.spark.transport.model.GetAsyncQueryResultActionRequest;
import org.opensearch.sql.spark.transport.model.GetAsyncQueryResultActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportGetAsyncQueryResultActionTest {

  @Mock private TransportService transportService;
  @Mock private ClusterService clusterService;
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
            transportService,
            new ActionFilters(new HashSet<>()),
            clusterService,
            jobExecutorService);
  }

  @Test
  public void testDoExecute() {
    GetAsyncQueryResultActionRequest request = new GetAsyncQueryResultActionRequest("jobId");
    AsyncQueryExecutionResponse asyncQueryExecutionResponse =
        new AsyncQueryExecutionResponse("IN_PROGRESS", null, null, null, null, null);
    when(jobExecutorService.getAsyncQueryResults(eq("jobId"), any()))
        .thenReturn(asyncQueryExecutionResponse);

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
                tupleValue(ImmutableMap.of("name", "Smith", "age", 30))),
            null,
            null,
            null);
    when(jobExecutorService.getAsyncQueryResults(eq("jobId"), any()))
        .thenReturn(asyncQueryExecutionResponse);

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
        .getAsyncQueryResults(eq("123"), any());

    action.doExecute(task, request, actionListener);

    verify(jobExecutorService, times(1))
        .getAsyncQueryResults(eq("123"), any(NullAsyncQueryRequestContext.class));
    verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception exception = exceptionArgumentCaptor.getValue();
    Assertions.assertTrue(exception instanceof RuntimeException);
    Assertions.assertEquals("JobId 123 not found", exception.getMessage());
  }

  @Test
  public void queryJobNotFound_translatesToResourceNotFoundException() {
    GetAsyncQueryResultActionRequest request = new GetAsyncQueryResultActionRequest("missing");
    doThrow(
            new org.opensearch.sql.job.exceptions.QueryJobNotFoundException(
                new org.opensearch.sql.job.QueryJobId("node-a", "ctx-x")))
        .when(jobExecutorService)
        .getAsyncQueryResults(eq("missing"), any());

    action.doExecute(task, request, actionListener);

    verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception captured = exceptionArgumentCaptor.getValue();
    // Transport-serializable OpenSearchException with status NOT_FOUND — survives cross-node
    // forwarding without being wrapped as NotSerializableExceptionWrapper(500).
    Assertions.assertTrue(
        captured instanceof org.opensearch.ResourceNotFoundException,
        "expected ResourceNotFoundException, got " + captured.getClass());
    Assertions.assertEquals(
        org.opensearch.core.rest.RestStatus.NOT_FOUND,
        ((org.opensearch.ResourceNotFoundException) captured).status());
  }

  @Test
  public void queryJobForbidden_translatesToOpenSearchStatusForbidden() {
    GetAsyncQueryResultActionRequest request = new GetAsyncQueryResultActionRequest("foreign");
    doThrow(new org.opensearch.sql.job.exceptions.QueryJobForbiddenException())
        .when(jobExecutorService)
        .getAsyncQueryResults(eq("foreign"), any());

    action.doExecute(task, request, actionListener);

    verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception captured = exceptionArgumentCaptor.getValue();
    Assertions.assertTrue(
        captured instanceof org.opensearch.OpenSearchStatusException,
        "expected OpenSearchStatusException, got " + captured.getClass());
    Assertions.assertEquals(
        org.opensearch.core.rest.RestStatus.FORBIDDEN,
        ((org.opensearch.OpenSearchStatusException) captured).status());
  }

  @Test
  public void localPplJobUsesLocalExecutorAndRendersSucceededRows() {
    QueryJobId id = new QueryJobId("local-node", "query-context");
    DiscoveryNode local = mock(DiscoveryNode.class);
    when(clusterService.localNode()).thenReturn(local);
    when(local.getId()).thenReturn(id.ownerNodeId());
    ExecutionEngine.Schema schema =
        new ExecutionEngine.Schema(
            ImmutableList.of(new ExecutionEngine.Schema.Column("count", null, INTEGER)));
    when(jobExecutorService.getAsyncQueryResults(eq(id.encode()), any()))
        .thenReturn(
            new AsyncQueryExecutionResponse(
                "SUCCEEDED",
                schema,
                ImmutableList.of(tupleValue(ImmutableMap.of("count", 3))),
                null,
                null,
                null));

    action.doExecute(task, new GetAsyncQueryResultActionRequest(id.encode()), actionListener);

    verify(jobExecutorService).getAsyncQueryResults(eq(id.encode()), any());
    verify(actionListener).onResponse(createJobActionResponseArgumentCaptor.capture());
    org.json.JSONObject response =
        new org.json.JSONObject(createJobActionResponseArgumentCaptor.getValue().getResult());
    Assertions.assertEquals("SUCCEEDED", response.getString("status"));
    Assertions.assertEquals(3, response.getJSONArray("datarows").getJSONArray(0).getInt(0));
    Assertions.assertEquals(1, response.getInt("total"));
  }

  @Test
  public void explainResponseIsRenderedWithExplainFormatter() {
    ExecutionEngine.ExplainResponseNodeV2 plan =
        new ExecutionEngine.ExplainResponseNodeV2("logical", "physical", null);
    plan.setLogicalTree(ImmutableMap.of("operator", "LogicalProject"));
    plan.setPhysicalTree(ImmutableMap.of("operator", "EnumerableCalc"));
    when(jobExecutorService.getAsyncQueryResults(eq("jobId"), any()))
        .thenReturn(
            new AsyncQueryExecutionResponse(
                "SUCCEEDED", null, null, null, null, new ExecutionEngine.ExplainResponse(plan)));

    action.doExecute(task, new GetAsyncQueryResultActionRequest("jobId"), actionListener);

    verify(actionListener).onResponse(createJobActionResponseArgumentCaptor.capture());
    org.json.JSONObject calcite =
        new org.json.JSONObject(createJobActionResponseArgumentCaptor.getValue().getResult())
            .getJSONObject("calcite");
    Assertions.assertEquals(
        "LogicalProject", calcite.getJSONObject("logical").getString("operator"));
    Assertions.assertEquals(
        "EnumerableCalc", calcite.getJSONObject("physical").getString("operator"));
  }

  @Test
  public void nonOwnerForwardsRequestAndRelaysResponseWithoutLocalExecution() {
    QueryJobId id = new QueryJobId("owner-node", "query-context");
    DiscoveryNode local = mock(DiscoveryNode.class);
    DiscoveryNode owner = mock(DiscoveryNode.class);
    ClusterState state = mock(ClusterState.class);
    DiscoveryNodes nodes = mock(DiscoveryNodes.class);
    when(clusterService.localNode()).thenReturn(local);
    when(local.getId()).thenReturn("entry-node");
    when(clusterService.state()).thenReturn(state);
    when(state.nodes()).thenReturn(nodes);
    when(nodes.get(id.ownerNodeId())).thenReturn(owner);
    GetAsyncQueryResultActionRequest request = new GetAsyncQueryResultActionRequest(id.encode());

    action.doExecute(task, request, actionListener);

    ArgumentCaptor<TransportResponseHandler<GetAsyncQueryResultActionResponse>> handlerCaptor =
        ArgumentCaptor.forClass(TransportResponseHandler.class);
    verify(transportService)
        .sendRequest(
            eq(owner),
            eq(TransportGetAsyncQueryResultAction.NAME),
            eq(request),
            eq(TransportRequestOptions.EMPTY),
            handlerCaptor.capture());
    GetAsyncQueryResultActionResponse response =
        new GetAsyncQueryResultActionResponse("{\"status\":\"RUNNING\"}");
    handlerCaptor.getValue().handleResponse(response);
    verify(actionListener).onResponse(response);
    verifyNoInteractions(jobExecutorService);
  }
}
