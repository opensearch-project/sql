/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

/** Transport implementation of DELETE {@code /_plugins/_ppl/jobs/{id}}. */
public class TransportPPLAsyncQueryDeleteAction extends TransportPPLAsyncQueryLifecycleAction {

  @Inject
  public TransportPPLAsyncQueryDeleteAction(
      TransportService transportService,
      ActionFilters actionFilters,
      NodeClient client,
      ClusterService clusterService,
      PPLAsyncQueryJobService jobService) {
    super(
        PPLAsyncQueryDeleteAction.NAME,
        transportService,
        actionFilters,
        client,
        clusterService,
        jobService);
  }

  @Override
  protected void handleLocal(
      TransportPPLQueryRequest request,
      User user,
      ActionListener<TransportPPLQueryResponse> listener) {
    listener.onResponse(
        PPLAsyncQueryResponseFormatter.deleted(
            jobService.cancelAndRemove(requestedId(request), user)));
  }
}
