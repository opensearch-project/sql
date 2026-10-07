/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.job.QueryJobId;
import org.opensearch.sql.job.exceptions.QueryJobForbiddenException;
import org.opensearch.sql.job.exceptions.QueryJobNotFoundException;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceImpl;
import org.opensearch.sql.spark.asyncquery.model.NullAsyncQueryRequestContext;
import org.opensearch.sql.spark.transport.model.CancelAsyncQueryActionRequest;
import org.opensearch.sql.spark.transport.model.CancelAsyncQueryActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportCancelAsyncQueryRequestActionTest {

  private static final QueryJobId JOB_ID = new QueryJobId("owner-node", "query-context");

  @Mock private TransportService transportService;
  @Mock private ClusterService clusterService;
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
            transportService,
            new ActionFilters(new HashSet<>()),
            clusterService,
            asyncQueryExecutorService);
  }

  @Test
  public void testDoExecute() {
    CancelAsyncQueryActionRequest request = new CancelAsyncQueryActionRequest(EMR_JOB_ID);
    when(asyncQueryExecutorService.cancelQuery(
            eq(EMR_JOB_ID), any(NullAsyncQueryRequestContext.class)))
        .thenReturn(EMR_JOB_ID);

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
    doThrow(new RuntimeException("Error"))
        .when(asyncQueryExecutorService)
        .cancelQuery(eq(EMR_JOB_ID), any(NullAsyncQueryRequestContext.class));

    action.doExecute(task, request, actionListener);

    Mockito.verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception exception = exceptionArgumentCaptor.getValue();
    Assertions.assertTrue(exception instanceof RuntimeException);
    Assertions.assertEquals("Error", exception.getMessage());
  }

  @Test
  public void localRunningPplJobIsDeletedAndAcknowledgedAsCancelled() {
    stubLocalNode(JOB_ID.ownerNodeId());
    when(asyncQueryExecutorService.cancelQuery(eq(JOB_ID.encode()), any())).thenReturn("CANCELLED");

    action.doExecute(task, new CancelAsyncQueryActionRequest(JOB_ID.encode()), actionListener);

    verify(actionListener).onResponse(deleteJobActionResponseArgumentCaptor.capture());
    Assertions.assertEquals(
        "{\"status\":\"CANCELLED\"}", deleteJobActionResponseArgumentCaptor.getValue().getResult());
    verifyNoForwarding();
  }

  @Test
  public void localTerminalPplJobIsDeletedAndAcknowledgedWithPriorStatus() {
    stubLocalNode(JOB_ID.ownerNodeId());
    when(asyncQueryExecutorService.cancelQuery(eq(JOB_ID.encode()), any())).thenReturn("SUCCEEDED");

    action.doExecute(task, new CancelAsyncQueryActionRequest(JOB_ID.encode()), actionListener);

    verify(actionListener).onResponse(deleteJobActionResponseArgumentCaptor.capture());
    Assertions.assertEquals(
        "{\"status\":\"SUCCEEDED\"}", deleteJobActionResponseArgumentCaptor.getValue().getResult());
  }

  @Test
  public void nonOwnerForwardsRequestAndRelaysResponseWithoutLocalExecution() {
    stubLocalNode("entry-node");
    DiscoveryNode owner = mock(DiscoveryNode.class);
    stubClusterNode(JOB_ID.ownerNodeId(), owner);
    CancelAsyncQueryActionRequest request = new CancelAsyncQueryActionRequest(JOB_ID.encode());

    action.doExecute(task, request, actionListener);

    ArgumentCaptor<TransportResponseHandler<CancelAsyncQueryActionResponse>> handlerCaptor =
        ArgumentCaptor.forClass(TransportResponseHandler.class);
    verify(transportService)
        .sendRequest(
            eq(owner),
            eq(TransportCancelAsyncQueryRequestAction.NAME),
            eq(request),
            eq(TransportRequestOptions.EMPTY),
            handlerCaptor.capture());
    CancelAsyncQueryActionResponse response =
        new CancelAsyncQueryActionResponse("{\"status\":\"CANCELLED\"}");
    handlerCaptor.getValue().handleResponse(response);
    verify(actionListener).onResponse(response);
    verifyNoInteractions(asyncQueryExecutorService);
  }

  @Test
  public void departedOwnerNodeFailsWithNotFound() {
    stubLocalNode("entry-node");
    stubClusterNode(JOB_ID.ownerNodeId(), null);

    action.doExecute(task, new CancelAsyncQueryActionRequest(JOB_ID.encode()), actionListener);

    verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    ResourceNotFoundException captured =
        Assertions.assertInstanceOf(
            ResourceNotFoundException.class, exceptionArgumentCaptor.getValue());
    Assertions.assertEquals(RestStatus.NOT_FOUND, captured.status());
    verifyNoInteractions(asyncQueryExecutorService);
    verifyNoForwarding();
  }

  @Test
  public void queryJobNotFound_translatesToResourceNotFoundException() {
    stubLocalNode(JOB_ID.ownerNodeId());
    doThrow(new QueryJobNotFoundException(JOB_ID))
        .when(asyncQueryExecutorService)
        .cancelQuery(eq(JOB_ID.encode()), any());

    action.doExecute(task, new CancelAsyncQueryActionRequest(JOB_ID.encode()), actionListener);

    verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    ResourceNotFoundException captured =
        Assertions.assertInstanceOf(
            ResourceNotFoundException.class, exceptionArgumentCaptor.getValue());
    Assertions.assertEquals(RestStatus.NOT_FOUND, captured.status());
  }

  @Test
  public void queryJobForbidden_translatesToOpenSearchStatusForbidden() {
    stubLocalNode(JOB_ID.ownerNodeId());
    doThrow(new QueryJobForbiddenException())
        .when(asyncQueryExecutorService)
        .cancelQuery(eq(JOB_ID.encode()), any());

    action.doExecute(task, new CancelAsyncQueryActionRequest(JOB_ID.encode()), actionListener);

    verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    OpenSearchStatusException captured =
        Assertions.assertInstanceOf(
            OpenSearchStatusException.class, exceptionArgumentCaptor.getValue());
    Assertions.assertEquals(RestStatus.FORBIDDEN, captured.status());
  }

  private void verifyNoForwarding() {
    verify(transportService, never())
        .sendRequest(
            any(DiscoveryNode.class),
            anyString(),
            any(TransportRequest.class),
            any(TransportRequestOptions.class),
            any(TransportResponseHandler.class));
  }

  private void stubLocalNode(String nodeId) {
    DiscoveryNode local = mock(DiscoveryNode.class);
    when(clusterService.localNode()).thenReturn(local);
    when(local.getId()).thenReturn(nodeId);
  }

  private void stubClusterNode(String nodeId, DiscoveryNode node) {
    ClusterState state = mock(ClusterState.class);
    DiscoveryNodes nodes = mock(DiscoveryNodes.class);
    when(clusterService.state()).thenReturn(state);
    when(state.nodes()).thenReturn(nodes);
    when(nodes.get(nodeId)).thenReturn(node);
  }
}
