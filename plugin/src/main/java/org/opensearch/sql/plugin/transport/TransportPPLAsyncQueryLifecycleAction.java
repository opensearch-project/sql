/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.json.JSONObject;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

/** Common owner-node routing and security-context handling for retained PPL jobs. */
abstract class TransportPPLAsyncQueryLifecycleAction
    extends HandledTransportAction<ActionRequest, TransportPPLQueryResponse> {
  private final String actionName;
  protected final PPLAsyncQueryJobService jobService;
  protected final NodeClient client;
  private final ClusterService clusterService;
  private final TransportService transportService;

  TransportPPLAsyncQueryLifecycleAction(
      String actionName,
      TransportService transportService,
      ActionFilters actionFilters,
      NodeClient client,
      ClusterService clusterService,
      PPLAsyncQueryJobService jobService) {
    super(actionName, transportService, actionFilters, TransportPPLQueryRequest::new);
    this.actionName = actionName;
    this.transportService = transportService;
    this.client = client;
    this.clusterService = clusterService;
    this.jobService = jobService;
  }

  @Override
  protected final void doExecute(
      Task task, ActionRequest request, ActionListener<TransportPPLQueryResponse> listener) {
    try {
      TransportPPLQueryRequest transportRequest =
          TransportPPLQueryRequest.fromActionRequest(request);
      String id = requestedId(transportRequest);
      PPLAsyncQueryJobId parsed = PPLAsyncQueryJobId.parse(id);
      if (!client.getLocalNodeId().equals(parsed.ownerNodeId())) {
        var ownerNode = clusterService.state().nodes().get(parsed.ownerNodeId());
        if (ownerNode == null) {
          listener.onFailure(
              new ResourceNotFoundException(
                  "PPL job owner node [" + parsed.ownerNodeId() + "] is not available"));
          return;
        }
        transportService.sendRequest(
            ownerNode,
            actionName,
            transportRequest,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, TransportPPLQueryResponse::new));
        return;
      }
      handleLocal(
          transportRequest, PPLAsyncQuerySecurity.currentUser(client.threadPool()), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  protected abstract void handleLocal(
      TransportPPLQueryRequest request,
      User user,
      ActionListener<TransportPPLQueryResponse> listener);

  protected static JSONObject json(TransportPPLQueryRequest request) {
    JSONObject json = request.getJsonContent();
    if (json == null) {
      throw new IllegalArgumentException("PPL asynchronous job request body is missing");
    }
    return json;
  }

  protected static String requestedId(TransportPPLQueryRequest request) {
    String id = json(request).optString("id", "").trim();
    if (id.isEmpty()) {
      throw new IllegalArgumentException("[id] must not be empty");
    }
    return id;
  }
}
