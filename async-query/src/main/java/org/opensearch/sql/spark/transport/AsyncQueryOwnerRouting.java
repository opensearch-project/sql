/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport;

import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.sql.job.QueryJobId;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

/**
 * Owner-node forwarding for async-query transport actions.
 *
 * <p>Both {@link TransportGetAsyncQueryResultAction} and {@link
 * TransportCancelAsyncQueryRequestAction} share the same forwarding contract: parse the incoming
 * queryId as {@link QueryJobId}, look up the owner {@link DiscoveryNode} in the current cluster
 * state, and forward the request to the owner via the same transport action name. The owner node's
 * handler then resolves the id in its local {@code QueryJobStore}.
 *
 * <p>Extracted here so the two transport actions do not each carry an identical copy.
 */
final class AsyncQueryOwnerRouting {

  private AsyncQueryOwnerRouting() {}

  /**
   * Forwards {@code request} to the node identified by {@code jobId.ownerNodeId()}.
   *
   * <p>Owner node absent from the current cluster state surfaces as {@link
   * ResourceNotFoundException} (a transport-serializable 404), so GET and DELETE agree on status
   * when the owner has left the cluster. Cross-node failures arrive wrapped in {@code
   * RemoteTransportException}; the listener is wrapped to unwrap the cause before returning, so
   * the client sees the real exception (and not the owner node's hostname/port).
   *
   * @param clusterService source of the current cluster state and node roster
   * @param transportService transport client used to issue the cross-node send
   * @param jobId parsed id whose {@code ownerNodeId} names the target node
   * @param actionName the same transport action name the entry node received; the owner has an
   *     identical handler registered under this name
   * @param request request received by the entry node; forwarded unchanged
   * @param responseReader deserializer for the response type
   * @param listener listener returning to the original caller
   * @param <RespT> concrete response type (each transport action has its own)
   */
  static <RespT extends ActionResponse> void forwardToOwner(
      ClusterService clusterService,
      TransportService transportService,
      QueryJobId jobId,
      String actionName,
      ActionRequest request,
      Writeable.Reader<RespT> responseReader,
      ActionListener<RespT> listener) {
    DiscoveryNode ownerNode = clusterService.state().nodes().get(jobId.ownerNodeId());
    if (ownerNode == null) {
      listener.onFailure(
          new ResourceNotFoundException("QueryId: " + jobId.encode() + " not found"));
      return;
    }
    ActionListener<RespT> unwrapping =
        ActionListener.wrap(
            listener::onResponse,
            failure -> {
              Throwable cause = ExceptionsHelper.unwrapCause(failure);
              listener.onFailure(
                  cause instanceof Exception ? (Exception) cause : new RuntimeException(cause));
            });
    transportService.sendRequest(
        ownerNode,
        actionName,
        request,
        TransportRequestOptions.EMPTY,
        new ActionListenerResponseHandler<>(unwrapping, responseReader, ThreadPool.Names.SAME));
  }
}
