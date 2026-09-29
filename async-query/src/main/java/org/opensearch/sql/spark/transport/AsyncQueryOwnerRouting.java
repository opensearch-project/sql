/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport;

import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.sql.job.QueryJobId;
import org.opensearch.sql.spark.asyncquery.exceptions.AsyncQueryNotFoundException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

/**
 * Owner-node forwarding for async-query transport actions.
 *
 * <p>Both {@link TransportGetAsyncQueryResultAction} and
 * {@link TransportCancelAsyncQueryRequestAction} share the same forwarding contract: parse the
 * incoming queryId as {@link QueryJobId}, look up the owner {@link DiscoveryNode} in the current
 * cluster state, and forward the request to the owner via the same transport action name. The
 * owner node's handler then resolves the id in its local {@code QueryJobStore}.
 *
 * <p>Extracted here so the two transport actions do not each carry an identical copy.
 */
final class AsyncQueryOwnerRouting {

  private AsyncQueryOwnerRouting() {}

  /**
   * Forwards {@code request} to the node identified by {@code jobId.ownerNodeId()}.
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
          new AsyncQueryNotFoundException("QueryId: " + jobId.encode() + " not found"));
      return;
    }
    transportService.sendRequest(
        ownerNode,
        actionName,
        request,
        TransportRequestOptions.EMPTY,
        new ActionListenerResponseHandler<>(listener, responseReader, ThreadPool.Names.SAME));
  }
}
