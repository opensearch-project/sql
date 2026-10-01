/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.job.QueryJobId;
import org.opensearch.sql.spark.asyncquery.exceptions.AsyncQueryNotFoundException;
import org.opensearch.sql.spark.transport.model.GetAsyncQueryResultActionRequest;
import org.opensearch.sql.spark.transport.model.GetAsyncQueryResultActionResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
class AsyncQueryOwnerRoutingTest {

  private static final QueryJobId JOB_ID = new QueryJobId("owner-node", "query-context");
  private static final String ACTION_NAME = TransportGetAsyncQueryResultAction.NAME;

  @Mock private ClusterService clusterService;
  @Mock private ClusterState clusterState;
  @Mock private DiscoveryNodes nodes;
  @Mock private DiscoveryNode owner;
  @Mock private TransportService transportService;
  @Mock private ActionListener<GetAsyncQueryResultActionResponse> listener;

  @Captor
  private ArgumentCaptor<TransportResponseHandler<GetAsyncQueryResultActionResponse>> handlerCaptor;

  @Test
  void departedOwnerReturnsNotFoundWithoutSendingRequest() {
    when(clusterService.state()).thenReturn(clusterState);
    when(clusterState.nodes()).thenReturn(nodes);
    when(nodes.get(JOB_ID.ownerNodeId())).thenReturn(null);

    forward(new GetAsyncQueryResultActionRequest(JOB_ID.encode()));

    ArgumentCaptor<Exception> failure = ArgumentCaptor.forClass(Exception.class);
    verify(listener).onFailure(failure.capture());
    assertEquals(AsyncQueryNotFoundException.class, failure.getValue().getClass());
    assertEquals("QueryId: " + JOB_ID.encode() + " not found", failure.getValue().getMessage());
    verifyNoInteractions(transportService);
  }

  @Test
  void forwardedResponseUsesWireReaderAndReturnsToOriginalListener() throws IOException {
    GetAsyncQueryResultActionRequest request =
        new GetAsyncQueryResultActionRequest(JOB_ID.encode());
    TransportResponseHandler<GetAsyncQueryResultActionResponse> handler =
        forwardToPresentOwner(request);
    GetAsyncQueryResultActionResponse response =
        new GetAsyncQueryResultActionResponse("{\"status\":\"SUCCEEDED\"}");

    try (BytesStreamOutput output = new BytesStreamOutput()) {
      response.writeTo(output);
      assertEquals(response.getResult(), handler.read(output.bytes().streamInput()).getResult());
    }
    assertEquals(ThreadPool.Names.SAME, handler.executor());
    handler.handleResponse(response);
    verify(listener).onResponse(response);
  }

  @Test
  void forwardedFailureReturnsToOriginalListener() {
    TransportResponseHandler<GetAsyncQueryResultActionResponse> handler =
        forwardToPresentOwner(new GetAsyncQueryResultActionRequest(JOB_ID.encode()));
    TransportException failure = new TransportException("owner unavailable");

    handler.handleException(failure);

    verify(listener).onFailure(failure);
  }

  private TransportResponseHandler<GetAsyncQueryResultActionResponse> forwardToPresentOwner(
      GetAsyncQueryResultActionRequest request) {
    when(clusterService.state()).thenReturn(clusterState);
    when(clusterState.nodes()).thenReturn(nodes);
    when(nodes.get(JOB_ID.ownerNodeId())).thenReturn(owner);

    forward(request);

    verify(transportService)
        .sendRequest(
            eq(owner),
            eq(ACTION_NAME),
            eq(request),
            eq(TransportRequestOptions.EMPTY),
            handlerCaptor.capture());
    return handlerCaptor.getValue();
  }

  private void forward(GetAsyncQueryResultActionRequest request) {
    AsyncQueryOwnerRouting.forwardToOwner(
        clusterService,
        transportService,
        JOB_ID,
        ACTION_NAME,
        request,
        GetAsyncQueryResultActionResponse::new,
        listener);
  }
}
